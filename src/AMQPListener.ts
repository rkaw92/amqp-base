import { Connection, Channel } from "amqplib";
import { IAMQPChannelManagerNoConfirms, AMQPChannelManager } from "./AMQPChannelManager";
import { IAMQPConsumer } from "./AMQPConsumer";

type ChannelType = Channel;

export interface ConsumerFactory {
    (channel: Channel): IAMQPConsumer;
};

export interface ConsumerConstructor {
    new(channel: Channel): IAMQPConsumer;
};

export type ConsumerConstructorOrFactory = ConsumerConstructor | ConsumerFactory;

export interface IAMQPListener {
    listen(): Promise<void>;
    stopListening(): Promise<void>;
};

type DestroyCallback = () => void;

/**
 * A Listener groups several Consumers together and coordinates the starting and stopping of their consumption.
 */
class AMQPListener implements IAMQPListener {
    private _connection: Connection;
    private _channelManager: IAMQPChannelManagerNoConfirms;
    private _consumerConstructors: ConsumerConstructorOrFactory[];
    private _consumers: IAMQPConsumer[];
    private _started: boolean;
    private _destroy: DestroyCallback | null;
    /**
     * Construct an AMQPListener.
     * @param {external:AMQPConnection} connection A connection, as obtained from the AMQPConnector.
     * @param {(Array.<function(external:AMQPChannel): module:AMQPBase.AMQPConsumer>)} consumerConstructors An array of functions (that accept a sole argument - a channel), which, when called, should each return an {@link module:AMQPBase.AMQPConsumer|AMQPConsumer}, perhaps with some "message" event handler already registered.
     */
    constructor(connection: Connection, consumerConstructors: ConsumerConstructorOrFactory[]) {
        this._connection = connection;
        this._channelManager = AMQPChannelManager.noConfirms(this._connection);
        this._consumerConstructors = consumerConstructors;
        this._consumers = [];
        this._started = false;
        this._destroy = null;
    }
    listen() {
        var self = this;
        
        if (self._started) {
            return Promise.resolve();
        }
        
        self._started = true;
        self._channelManager.start();
        
        return new Promise<void>(function(resolve, reject) {
            function channelCreated(channel: ChannelType) {
                self._consumers = [];
                var consumePromises: Promise<void>[] = [];
                self._consumerConstructors.forEach(function createConsumer(consumerConstructor) {
                    // NOTE: This is a bit of abuse because a non-constructor function will also work with "new", even if
                    //  it just returns a consumer instance instead of initializing "this".
                    var consumer = new (consumerConstructor as ConsumerConstructor)(channel);
                    self._consumers.push(consumer);
                    consumePromises.push(consumer.consume());
                    // If the consumer is cancelled by the server, make sure to reinstate it, unless it is being stopped manually:
                    consumer.on('cancel', function() {
                        //TODO: Add some form of logging here.
                        if (!consumer.isStopping()) {
                            consumer.consume();
                        }
                    });
                });
                
                Promise.all(consumePromises).then(() => resolve(), function handleConsumerFailure(error) {
                    // We explicitly choose to do nothing - this is already going to be handled by the channel manager.
                    // Namely, when the channel is re-created due to the error which caused the consumer failure, the channelCreated function is going to be called.
                });
            }
            
            function channelClosed(channel: ChannelType) {
                self._consumers.forEach(function(consumer) {
                    return consumer.stopConsuming();
                });
            }
            
            self._channelManager.on('create', channelCreated);
            self._channelManager.on('close', channelClosed);
            
            self._destroy = function destroy() {
                self._channelManager.off('create', channelCreated);
                self._channelManager.off('close', channelClosed);
                reject(new Error('Manually stopped listening before it could start'));
            };
        });
    }


    /**
     * Stop listening on all consumers.
     * @returns {Promise} a promise which fulfills when all consumers have ceased consumption.
     */
    stopListening() {
        var self = this;
        if (self._started) {
            self._started = false;
            self._destroy!();
        }
        return Promise.all(self._consumers.map(function(consumer) {
            return consumer.stopConsuming();
        })).then(function() {
            self._channelManager.stop();
        });
    }
}

export { AMQPListener };
