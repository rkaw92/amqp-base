import { Channel, ConfirmChannel, Message } from "amqplib";
import { AsyncEventEmitter } from "./AsyncEventEmitter";
import { Binding } from "./Binding";
import { ConsumeOptions } from "./ConsumeOptions";
import { EmitterOf } from "./EmitterOf";
import { Exchange } from "./Exchange";
import { QueueOptions } from "./QueueOptions";

export interface MessageOperations {
    ack(): void;
    requeue(): void;
    reject(): void;
};

type AMQPConsumerEmitter = EmitterOf<"message",[ message: Message, ops: MessageOperations ]> & EmitterOf<"cancel",[ details: { initiator: string } ]>;

export interface IAMQPConsumer extends AMQPConsumerEmitter {
    consume(): Promise<void>;
    stopConsuming(): Promise<void>;
    isStopping(): boolean;
}

export interface AMQPConsumerOptions {
    queue?: QueueOptions;
    consume?: ConsumeOptions;
    exchanges?: Exchange[];
    binds?: Binding[];
};

class AMQPConsumer extends AsyncEventEmitter implements IAMQPConsumer {
    private _channel: Channel | ConfirmChannel;
    private _queueName: string;
    private _options: AMQPConsumerOptions;
    private _queueOptions: Partial<QueueOptions>;
    private _consumeOptions: Partial<ConsumeOptions>;
    private _exchanges: Exchange[];
    private _binds: Binding[];
    // TODO: Vanquish the unknowns!
    private _consumerTag: string | null;
    private _started: boolean;
    private _consumePromise: Promise<void> | null;
    private _stopPromise: Promise<void> | null;
    constructor(channel: Channel | ConfirmChannel, queueName: string, options: AMQPConsumerOptions = {}) {
        super();
        this._channel = channel;
        this._queueName = queueName;
        this._options = options || {};
        this._queueOptions = options.queue || {};
        this._consumeOptions = options.consume || {};
        this._exchanges = options.exchanges || [];
        this._binds = options.binds || [];
    
        this._consumerTag = null;
        this._started = false;
        this._consumePromise = null;
        this._stopPromise = null;
    }

    async _startConsuming() {
        const channel = this._channel;
        const queueName = this._queueName;
        const queueOptions = this._queueOptions;
        const consumeOptions = this._consumeOptions;
        const exchanges = this._exchanges;
        const binds = this._binds;
        const emitAsync = this.emitAsync.bind(this);
        const self = this;

        // First, we need to make sure that our queue exists:
        const queueInfo = await channel.assertQueue(queueName, queueOptions);
        // Store the actual queue name, in case the caller did not specify one and the server generates one for us:
        const actualQueueName = queueInfo.queue;
        // Create all of the required exchanges and bind queues to them:
        await Promise.all(exchanges.map(function establishExchange(exchangeDefinition) {
            return channel.assertExchange(exchangeDefinition.name, exchangeDefinition.type, exchangeDefinition.options || {});
        }));
        await Promise.all(binds.map(function establishBind(bindDefinition) {
            return channel.bindQueue(actualQueueName, bindDefinition.exchange, bindDefinition.pattern, bindDefinition.options || {});
        }));
        // Note: below, we fire off two AMQP RPCs one after another within our channel, so that we can make sure they are executed without any interleaving.
        // This is to ensure that our prefetch() setting, if any, will not have been overridden in the meantime (by, for example, establishConsumer() call entries from other consumers).
        // Within the channel scope, the basic.qos (prefetch) must take place first, because RPCs are synchronous (non-pipelined) in AMQP 0-9-1.
        var consumptionPromises = [];

        if (consumeOptions.prefetch) {
            consumptionPromises.push(channel.prefetch(consumeOptions.prefetch));
        }

        consumptionPromises.push(channel.consume(actualQueueName, function handleMessage(message) {
            // If the message is a consumer cancellation notification from RabbitMQ, inform the listeners of it:
            if (message === null) {
                self._started = false;
                self._consumePromise = null;
                /**
                 * The consumer has been cancelled by the server because the queue has been deleted or has failed over (if mirrored).
                 * @event module:AMQPBase.AMQPConsumer#cancel
                 */
                emitAsync('cancel', { initiator: 'server' });
                return;
            }
            // Define "reaction functions" to pass along with the message.
            // NOTE: We can't use channel.ack.bind() because that allows passing
            //  arbitrary arguments to channel.ack() et al. - see issue #8.
            const ack = function ack() { channel.ack(message); };
            const requeue = function requeue() { channel.reject(message, true); };
            const reject = function reject() { channel.reject(message, false); };
            /**
             * A message has been received.
             * @event module:AMQPBase.AMQPConsumer#message
             * @param {Object} message An object representing the message's payload and metadata.
             * @param {Buffer} message.content The payload of the message.
             * @param {Object} message.properties Message metadata.
             * @param {(Object.<string,function>)} operations A key-value map of functions representing various processing outcomes on this message. One of these must be called by the event handler to actually finish consuming the message.
             * @param {function} operations.ack Send a success acknowledgement, meaning that the message has been fully dealt with and should not be re-sent.
             * @param {function} operations.requeue Return the message to the queue it was obtained from, so that it may be re-delivered.
             * @param {function} operations.reject Reject the message, destroying it or, if a dead-letter-exchange has been configured for the consumer's queue, re-routing it to the dead letter exchange.
             */
            emitAsync('message', message, {
                ack: ack,
                requeue: requeue,
                reject: reject
            });
        }, consumeOptions).then(function(consumeResult) {
            var consumerTag = consumeResult.consumerTag;
            self._consumerTag = consumerTag;
        }));
        self._started = true;
    }

    /**
     * Prepare for and start the consumption. This also declares the queue and the exchanges/bindings, if any.
     * Subsequent calls to this method are de-duplicated and return the same promise, even though the consumer is only started once.
     * @returns a promise which fulfills when the consumption has started and rejects if an error has occured during preparation.
     */
    consume(): Promise<void> {
        if (!this._started) {
            try {
                this._consumePromise = this._startConsuming();
            } catch (error) {
                return Promise.reject(error);
            }
        }
    
        return this._consumePromise!;
    }

    stopConsuming(): Promise<void> {
        var self = this;
        var currentConsumerTag = self._consumerTag;
    
        // If the consumer has not even been started, has been stopped, or is currently stopping, there is nothing to do:
        if (!self._started || self._stopPromise || !self._consumePromise) {
            return self._stopPromise || Promise.resolve();
        }
    
        self._started = false;
    
        // We must be certain that the client really is consuming (i.e. has a consumer tag) before trying to cancel the consumer:
        self._stopPromise = self._consumePromise.then(function cancelCurrentConsumer() {
            return self._channel.cancel(currentConsumerTag!);
        }).catch(function _ignoreCancellationErrors() {
            // no-op: we need to eat errors because the channel may very well be closed
            // After all, the caller wants us not to consume - a closed channel
            //  indirectly causes this outcome.
        }).then(function() {
            self._stopPromise = null;
        });
        self._consumePromise = null;
    
        return self._stopPromise;
    }

    isStopping() {
        return (this._stopPromise !== null);
    }
}

export { AMQPConsumer };
