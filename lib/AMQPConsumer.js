/**
 * @module AMQPConsumer
 */

var EventEmitter = require('events').EventEmitter;
var when = require('when');


/**
 * An instance of AMQPConsumer controls a single subscription on a single queue.
 * This corresponds 1:1 to an AMQP consumer, since only a single consumer tag is held at a time.
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

AMQPConsumer.prototype.consume = function consume() {
	var channel = this._channel;
	var queueName = this._queueName;
	var queueOptions = this._queueOptions;
	var consumeOptions = this._consumeOptions;
	var exchanges = this._exchanges;
	var binds = this._binds;
	var emit = this.emit.bind(this);
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
					emit('cancel', { initiator: 'server' });
					return;
				}
				var ack = channel.ack.bind(channel, message);
				var requeue = channel.reject.bind(channel, message, true);
				var reject = channel.reject.bind(channel, message, false);
				emit('message', message, {
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
	}).then(function() {
		self._stopPromise = null;
	});
	self._consumePromise = null;
	
	return self._stopPromise;
};

AMQPConsumer.prototype.isStopping = function isStopping() {
	return (this._stopPromise !== null);
};

module.exports = AMQPConsumer;
