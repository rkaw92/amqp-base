/**
 * @module AMQPListener
 */

var AMQPChannelManager = require('./AMQPChannelManager');
var when = require('when');

/**
 * A Listener groups several Consumers together, each representing a "tier" of processing, and presents them to the user as a manageable whole.
 * A tier is a queue with specific consumption behaviour with regard to waiting after a failure.
 * By nature, AMQP consumers always run at full throughput - that is, messages are sent as soon as possible, and processing is not delayed.
 * However, for some problematic messages, it is desirable to delay retrying so as to ease the load on the system.
 * This is implemented using tiers (queues), where every but the last tier has a "dead letter exchange" set, so that rejected messages
 *  fall through to the next queue.
 */
function AMQPListener(connection, consumerConstructors) {
	this._connection = connection;
	this._consumerConstructors = consumerConstructors;
	this._consumers = [];
	this._started = false;
	this._destroy = null;
}

AMQPListener.prototype.listen = function listen() {
	var self = this;
	
	if (self._started) {
		return;
	}
	
	self._started = true;
	self._channelManager = new AMQPChannelManager(self._connection);
	self._channelManager.start();
	
	return when.promise(function(resolve, reject) {
		function channelCreated(channel) {
			self._consumers = [];
			var consumerPromises = [];
			self._consumerConstructors.forEach(function createConsumer(consumerConstructor) {
				var consumer = new consumerConstructor(channel);
				self._consumers.push(consumer);
				consumerPromises.push(consumer.consume());
				// If the consumer is cancelled by the server, make sure to reinstate it, unless it is being stopped manually:
				consumer.on('cancel', function() {
					//TODO: Add some form of logging here.
					if (!consumer.isStopping()) {
						consumer.consume();
					}
				});
			});
			
			when.all(consumerPromises).done(resolve, function handleConsumerFailure(error) {
				// We explicitly choose to do nothing - this is already going to be handled by the channel manager.
				// Namely, when the channel is re-created due to the error which caused the consumer failure, the channelCreated function is going to be called.
			});
		}
		
		function channelClosed(channel) {
			self._consumers.forEach(function(consumer) {
				return consumer.stopConsuming();
			});
		}
		
		self._channelManager.on('create', channelCreated);
		self._channelManager.on('close', channelClosed);
		
		self._destroy = function destroy() {
			self._channelManager.removeListener('create', channelCreated);
			self._channelManager.removeListener('close', channelClosed);
			reject(new Error('Manually stopped listening before it could start'));
		};
	});
};

AMQPListener.prototype.stopListening = function stopListening() {
	if (this._started) {
		this._started = false;
		this._destroy();
	}
	return when.all(this._consumers.map(function(consumer) {
		return consumer.stopConsuming();
	}));
};

module.exports = AMQPListener;
