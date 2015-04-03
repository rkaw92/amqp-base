/**
 * @module AMQPChannelManager
 */

var EventEmitter = require('events').EventEmitter;
var when = require('when');

/**
 * An AMQP Channel Manager is a state machine which uses a single AMQP connection to maintain a single channel open.
 * It automatically re-creates the channel in case it is dropped, unless the connection has also encountered an error.
 */
function AMQPChannelManager(connection) {
	EventEmitter.call(this);
	this._connection = connection;
	this._started = false;
	this._creatingChannel = false;
	this._channel = null;
	this._destroy = null;
	
	this._log = function log(occurence) {
		console.log('[%s] AMQPChannelManager:', (new Date()).toISOString(), occurence);
	};
	
	// If our underlying connection aborts, we need to destroy the channel (i.e. cease our re-creation attempts).
	connection.once('close', (function() {
		this._destroyChannel();
		this._connectionClosed = true;
	}).bind(this));
	connection.once('error', (function() {
		this._destroyChannel();
		this._connectionClosed = true;
	}).bind(this));
}
AMQPChannelManager.prototype = Object.create(EventEmitter.prototype);

AMQPChannelManager.prototype._createChannel = function _createChannel() {
	var self = this;
	var connection = self._connection;
	
	
	// If we're not started, or we already are in the middle of creating a channel, prevent the operation from taking place:
	if (!self._started || self._creatingChannel) {
		return;
	}
	
	self._creatingChannel = true;
	when.try(connection.createChannel.bind(connection)).done(function channelCreated(channel) {
		var channelDropped = false;
		var channelDestroyed = false;
		self._channel = channel;
		self._creatingChannel = false;
		self._destroy = function destroyChannel() {
			channel.close();
		};
		self.emit('create', channel);
		
		function handleChannelEvent(closeType, eventData) {
			if (channelDropped) {
				return;
			}
			
			self._channel = null;
			self.emit('close', channel);
			channelDropped = true;
			
			// Try re-creating the channel unless somebody told us not to do it:
			if (!channelDestroyed) {
				//TODO: Come up with an actual retry strategy:
				self._recreateTimeout = setTimeout(function() {
					self._createChannel();
				}, 3000);
			}
		}
		
		var handleError = handleChannelEvent.bind(undefined, 'error');
		var handleClose = handleChannelEvent.bind(undefined, 'close');
		
		channel.on('error', handleError);
		channel.on('close', handleClose);
	}, function channelCreationFailed(error) {
		self._creatingChannel = false;
		self._log(error);
		self._recreateTimeout = setTimeout(function() {
			self._createChannel();
		}, 5000);
	});
};

AMQPChannelManager.prototype._destroyChannel = function _destroyChannel() {
	if (this._channel) {
		this._destroy();
		this._channel = null;
	}
	clearTimeout(this._recreateTimeout);
};

AMQPChannelManager.prototype.start = function start() {
	if (this._connectionClosed) {
		throw new Error('The connection has been closed - can not start an AQMPChannelManager on a closed connection!');
	}
	if (!this._started) {
		this._started = true;
		this._createChannel();
	}
};

AMQPChannelManager.prototype.stop = function stop() {
	if (this._started) {
		this._started = false;
		this._destroyChannel();
	}
};

module.exports = AMQPChannelManager;
