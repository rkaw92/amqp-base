var AMQPChannelManager = require('./AMQPChannelManager');
var when = require('when');

/**
 * A Listener groups several Consumers together and coordinates the starting and stopping of their consumption.
 * @constructor
 * @memberof module:AMQPBase
 * @param {external:AMQPConnection} connection A connection, as obtained from the AMQPConnector.
 * @param {(Array.<function(external:AMQPChannel): module:AMQPBase.AMQPConsumer>)} consumerConstructors An array of functions (that accept a sole argument - a channel), which, when called, should each return an {@link module:AMQPBase.AMQPConsumer|AMQPConsumer}, perhaps with some "message" event handler already registered.
 */
function AMQPListener(connection, consumerConstructors) {
	this._connection = connection;
	this._channelManager = new AMQPChannelManager(this._connection);
	this._consumerConstructors = consumerConstructors;
	this._consumers = [];
	this._started = false;
	this._destroy = null;
}

/**
 * Start listening by creating consumers from the consumer factory functions and initiating their consumption processes.
 * @returns {Promise} a promise which fulfills when all of the created consumers have started consuming, and rejects if the initialization has been interrupted using {@link module:AMQPBase.AMQPListener#stopListening|stopListening()}.
 */
AMQPListener.prototype.listen = function listen() {
	var self = this;
	
	if (self._started) {
		return;
	}
	
	self._started = true;
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

/**
 * Stop listening on all consumers.
 * @returns {Promise} a promise which fulfills when all consumers have ceased consumption.
 */
AMQPListener.prototype.stopListening = function stopListening() {
	var self = this;
	if (self._started) {
		self._started = false;
		self._destroy();
	}
	return when.all(self._consumers.map(function(consumer) {
		return consumer.stopConsuming();
	})).then(function() {
		self._channelManager.stop();
	});
};

module.exports = AMQPListener;
