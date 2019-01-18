var EventEmitter = require('./AsyncEventEmitter');
var when = require('when');

/**
 * An exchange definition, reflecting an AMQP exchange object to create at the broker's side.
 * @typedef {Object} module:AMQPBase.AMQPConsumer~Exchange
 * @property {string} name The name of the exchange.
 * @property {string} type Exchange type. One of "direct", "topic", "fanout", "headers".
 * @property {Object} [options] Additional options concerning the properties of the exchange to create.
 * @property {boolean} [options.durable=true] Whether the exchange declaration should survive a restart of the AMQP broker.
 */

/**
 * A binding definition, representing a binding of a queue to an exchange. The queue name that is bound is implied from the context it is used in.
 * Namely, when passed to an {@link module:AMQPBase.AMQPConsumer|AMQPConsumer}, it regards the consumer's queue.
 * @typedef {Object} module:AMQPBase.AMQPConsumer~Binding
 * @property {string} exchange Exchange name to bind to, corresponding to {@link module:AMQPBase.AMQPConsumer~Exchange#name}.
 * @property {string} pattern The pattern to use when binding. Its syntax depends on the exchange type. For topic exchanges, placeholders # and * are allowed, as per AMQP spec.
 * @property {Object} [options] Additional options to use when binding. Only used for specific exchange types, such as "headers".
 */

/**
 * An instance of AMQPConsumer controls a single subscription on a single queue.
 * This corresponds 1:1 to an AMQP consumer, since only a single consumer tag is held at a time.
 * Upon starting, the consumer attempts to declare all the required objects on the broker - at least the queue, plus the exchange(s) and bind(s), if any were specified.
 * Any failures or conflicting declarations at creation will take down the entire channel, so an upper layer must detect this condition and re-create the consumer.
 * For this reason, direct use of the AMQPConsumer is discouraged. An {@link module:AMQPBase.AMQPListener|AMQPListener} handles this channel re-instatement automatically and reconstructs the consumers as requested.
 * @constructor
 * @memberof module:AMQPBase
 * @extends EventEmitter
 *
 * @param {external:AMQPChannel} channel The channel which the consumer shall use. The channel must be open - attempts to use a channel that is closed will result in failure.
 * @param {string} queueName Name of the queue that this consumer shall consume from.
 * @param {Object} [options] Settings controlling the behaviour of the consumer. All but the most trivial consumers will want to use this.
 *
 * @param {Object} [options.queue] Queue options, controlling the properties of the queue when declaring it on the broker.
 * @param {boolean} [options.queue.durable=true] Whether the queue should be declared as durable (true) or transient (false).
 * @param {boolean} [options.queue.exclusive=false] Whether this queue is exclusive to the channel.
 * @param {boolean} [options.queue.autoDelete=false] Whether the queue should be deleted when the channel is closed.
 *
 * @param {Object} [options.consume] Consumption options, which can alter the behaviour of the consumption process itself.
 * @param {boolean} [options.consume.exclusive=false] Whether the consumption process should acquire an exclusive lock on the queue, preventing other consumers from using it at the same time.
 * @param {boolean} [options.consume.prefetch=0] A limit of how many unacked (outstanding) messages the consumer may hold at any given time. Zero means no limit. Note that, on RabbitMQ before version 3.3.0, it applies per-channel, not per-consumer, so the limit is shared between multiple consumers.
 *
 * @param {module:AMQPBase.AMQPConsumer~Exchange[]} [options.exchanges] Definitions of exchanges that should be declared at the broker's side before starting consumption.
 *
 * @param {module:AMQPBase.AMQPConsumer~Binding[]} [options.binds] Queue bindings that should be established prior to consuming messages.
 */
function AMQPConsumer(channel, queueName, options) {
	EventEmitter.call(this);
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
AMQPConsumer.prototype = Object.create(EventEmitter.prototype);

/**
 * Prepare for and start the consumption. This also declares the queue and the exchanges/bindings, if any.
 * Subsequent calls to this method are de-duplicated and return the same promise, even though the consumer is only started once.
 * @returns {Promise} a promise which fulfills when the consumption has started and rejects if an error has occured during preparation.
 */
AMQPConsumer.prototype.consume = function consume() {
	var channel = this._channel;
	var queueName = this._queueName;
	var queueOptions = this._queueOptions;
	var consumeOptions = this._consumeOptions;
	var exchanges = this._exchanges;
	var binds = this._binds;
	var emitAsync = this.emitAsync.bind(this);
	var self = this;
	// Store the actual queue name, in case the caller did not specify one and the server generates one for us:
	var actualQueueName;

	if (!self._started) {
		// First, we need to make sure that our queue exists:
		self._consumePromise = when.try(channel.assertQueue.bind(channel), queueName, queueOptions).then(function rememberActualQueueName(queueInfo) {
			actualQueueName = queueInfo.queue;
		}).then(function establishExchanges() {
			return when.all(exchanges.map(function establishExchange(exchangeDefinition) {
				return channel.assertExchange(exchangeDefinition.name, exchangeDefinition.type, exchangeDefinition.options || {});
			}));
		}).then(function establishBinds() {
			return when.all(binds.map(function establishBind(bindDefinition) {
				return channel.bindQueue(actualQueueName, bindDefinition.exchange, bindDefinition.pattern, bindDefinition.options || {});
			}));
		}).then(function establishConsumer() {
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
				var ack = function ack() { channel.ack(message); };
				var requeue = function requeue() { channel.reject(message, true); };
				var reject = function reject() { channel.reject(message, false); };
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
		});
		self._started = true;
	}

	return self._consumePromise;
};

/**
 * Stop consuming messages from the designated queue. If the queue is an autoDelete queue, it may still not be deleted after this completes, as long as the channel remains open.
 * Subsequent calls to this method are de-duplicated and return the same promise.
 * @returns {Promise} a promise which fulfills when the consumption has stopped (i.e. no new messages can be emitted as events) and rejects if the cancellation has failed.
 */
AMQPConsumer.prototype.stopConsuming = function stopConsuming() {
	var self = this;
	var currentConsumerTag = self._consumerTag;

	// If the consumer has not even been started, has been stopped, or is currently stopping, there is nothing to do:
	if (!self._started || self._stopPromise) {
		return self._stopPromise || when.resolve();
	}

	self._started = false;

	// We must be certain that the client really is consuming (i.e. has a consumer tag) before trying to cancel the consumer:
	self._stopPromise = self._consumePromise.then(function cancelCurrentConsumer() {
		return self._channel.cancel(currentConsumerTag);
	}).catch(function _ignoreCancellationErrors() {
		// no-op: we need to eat errors because the channel may very well be closed
		// After all, the caller wants us not to consume - a closed channel
		//  indirectly causes this outcome.
	}).then(function() {
		self._stopPromise = null;
	});
	self._consumePromise = null;

	return self._stopPromise;
};

/**
 * Check whether the consumer is in the process of stopping consumption.
 * @returns {boolean}
 */
AMQPConsumer.prototype.isStopping = function isStopping() {
	return (this._stopPromise !== null);
};

module.exports = AMQPConsumer;
