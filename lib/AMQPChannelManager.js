var EventEmitter = require('./AsyncEventEmitter');
var when = require('when');

/**
 * An AMQP Channel Manager is a state machine which uses a single AMQP connection to maintain a single channel open.
 * It automatically re-creates the channel in case it is dropped, unless the connection has also encountered an error.
 * @constructor
 * @memberof module:AMQPBase
 * @extends EventEmitter
 * @param {external:AMQPConnection} connection An established AMQP connection, such as one obtained from an AMQPConnector's "connect" event.
 * @param {Object} [options] Additional settings, altering the channel manager's behaviour.
 * @param {boolean} [options.confirm=false] Whether the created channels should be put in confirm mode. Iff true, the #publish method of the emitted channels accepts an additional argument - a Node-style callback which executes when the message has been published.
 */
function AMQPChannelManager(connection, options) {
	EventEmitter.call(this);
	this._connection = connection;
	this._started = false;
	this._creatingChannel = false;
	this._channel = null;
	this._destroy = null;
	this._options = options || {};
	if (typeof(this._options.confirm) === 'undefined') {
		this._options.confirm = false;
	}

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
	var createChannel;

	if (this._options.confirm) {
		createChannel = connection.createConfirmChannel.bind(connection);
	}
	else {
		createChannel = connection.createChannel.bind(connection);
	}
	when.try(createChannel).done(function channelCreated(channel) {
		var channelDropped = false;
		var channelDestroyed = false;
		self._channel = channel;
		self._creatingChannel = false;
		self._destroy = function destroyChannel() {
			channel.close();
		};
		/**
		 * A channel has been opened for communicating with the AMQP broker. Commands can be issued through it.
		 * @event module:AMQPBase.AMQPChannelManager#create
		 * @param {external:AMQPChannel} channel An object representing the channel. Can be passed to other components requiring an open channel (e.g. {@link module:AMQPBase.AMQPConsumer|AMQPConsumer}).
		 * @see {@link http://www.squaremobius.net/amqp.node/doc/channel_api.html} for documentation regarding the Channel API.
		 */
		self.emitAsync('create', channel);

		function handleChannelEvent(closeType, eventData) {
			if (channelDropped) {
				return;
			}

			self._channel = null;
			channelDropped = true;

			/**
			 * The previously-opened channel has been closed due to an error or a deliberate request using {@link module:AMQPBase.AMQPChannelManager#stop|stop()}.
			 * @event module:AMQPBase.AMQPChannelManager#close
			 * @param {external:AMQPChannel} channel The channel which has been closed and is no longer suitable for communicating with the broker.
			 */
			self.emitAsync('close', channel);


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

/**
 * Start maintaining an open channel. This requires that the connection still be open.
 * @throws {Error} if the connection is closed.
 * @fires module:AMQPBase.AMQPChannelManager#create
 */
AMQPChannelManager.prototype.start = function start() {
	if (this._connectionClosed) {
		throw new Error('The connection has been closed - can not start an AQMPChannelManager on a closed connection!');
	}
	if (!this._started) {
		this._started = true;
		this._createChannel();
	}
};

/**
 * Stop maintaining an open channel and, if already opened, close it.
 * @fires module:AMQPBase.AMQPChannelManager#destroy
 */
AMQPChannelManager.prototype.stop = function stop() {
	if (this._started) {
		this._started = false;
		this._destroyChannel();
	}
};

module.exports = AMQPChannelManager;
