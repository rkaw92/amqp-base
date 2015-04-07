var EventEmitter = require('events').EventEmitter;
var amqplib = require('amqplib');
var when = require('when');

/**
 * An AMQP Connector instance encapsulates a series of connections to an AMQP broker.
 * It emits a "connect" event every time a new connection is established,
 * and "disconnect" whenever an old connection is closed for whatever reason.
 * It is meant for building long-running applications relying on AMQP reconnection.
 * @constructor
 * @memberof module:AMQPBase
 * @extends EventEmitter
 * @param {(string|string[])} serverURI The AMQP URI of the broker which the connector should connect to, in the format: `amqp://user:password@host.tld:port/vhost`. If an array of URIs are passed, a failed connection (at any time) will result in attempting to connect to the next URI in the list, in a round-robin fashion.
 * @param {Object} [options] Optional settings, used to control the behaviour of the connector as well as of its underlying mechanisms, such as TCP network sockets.
 * @param {Object} [options.socket] Optional socket settings, passed to the socket library (the latter being `net` or `tls`, depending on the URI).
 * @param {Object} [options.socket.noDelay=true] An additional socket setting, enabling or disabling the Nagle algorithm (using the TCP_NODELAY socket option).
 */
function AMQPConnector(serverURI, options) {
	EventEmitter.call(this);
	
	// Initial settings:
	this._serverURIs = (Array.isArray(serverURI)) ? serverURI.slice() : [ serverURI ];
	options = options || {};
	options.socket = options.socket || {};
	this._options = options;
	
	// State-keeping:
	this._state = {
		started: false,
		connecting: false,
		connection: null,
		lastUsedURI: null,
		reconnectTimeout: null,
		destroy: null
	};
	
	this._log = function log(occurence) {
		console.log('[%s] AMQPConnector:', (new Date()).toISOString(), occurence);
	};
}
AMQPConnector.prototype = Object.create(EventEmitter.prototype);

AMQPConnector.prototype._selectServerURI = function _selectServerURI(lastUsedURI) {
	var lastURIIndex = this._serverURIs.indexOf(lastUsedURI);
	// If the last used URI is not in the list at all, just use the first URI available:
	if (lastURIIndex < 0) {
		return this._serverURIs[0];
	}
	// If the URI is in the list and is not its last element, use the first element after it:
	if (lastURIIndex < (this._serverURIs.length - 1)) {
		return this._serverURIs[lastURIIndex + 1];
	}
	else {
		// The URI was the last one in the list. Start from the beginning.
		return this._serverURIs[0];
	}
};

AMQPConnector.prototype._createConnection = function _createConnection() {
	var self = this;
	var state = this._state;
	
	// Guard clause: if we are not started (yet/anymore), or already trying to connect, do nothing. This should end a reconnection chain and prevent multiple concurrent connections.
	if (!state.started || state.connecting) {
		return;
	}
	
	var URI = self._selectServerURI(state.lastUsedURI);
	state.lastUsedURI = URI;
	state.connecting = true;
	when.try(amqplib.connect, URI, self._options).done(function connectionSucceeded(connection) {
		var connectionDropped = false;
		var connectionDestroyed = false;
		state.connection = connection;
		state.connecting = false;
		state.destroy = function destroyThisConnection() {
			connection.close();
			connectionDestroyed = true;
		};
		/**
		 * A connection has been established and is ready to be used.
		 * Note that it is usually required to open a channel over the connection to actually talk to the AMQP broker.
		 * This event also fires when an automatic reconnection has been completed. Note that the connection object will be different each time it is fired.
		 * @event module:AMQPBase.AMQPConnector#connect
		 * @param {external:AMQPConnection} connection An AMQP connection, suitable for passing into other components which require a connection object (e.g. {@link module:AMQPBase.AMQPChannelManager|AMQPChannelManager}).
		 */
		self.emit('connect', connection);
		
		function handleConnectionEvent(closeType, eventData) {
			// Guard clause: do not react to the connection closing more than one time:
			if (connectionDropped) {
				return;
			}
			
			state.connection = null;
			connectionDropped = true;
			self._log({ event: closeType, eventData: eventData });
			self._log('connection dropped (' + closeType + '); attempting to reconnect');
			
			/**
			 * A connection has been closed, either because of network conditions or manually using {@link module:AMQPBase.AMQPConnector#stop|stop()}.
			 * @event module:AMQPBase.AMQPConnector#disconnect
			 * @param {external:AMQPConnection} connection The connection which has been closed and is no longer suitable for use.
			 * @see {@link http://www.squaremobius.net/amqp.node/doc/channel_api.html} for documentation regarding the Connection API.
			 */
			self.emit('disconnect', connection);
			
			// Now, try and reconnect, unless we told ourselves not to:
			if (!connectionDestroyed) {
				state.reconnectTimeout = setTimeout(function() {
					self._createConnection();
				}, 1000);
			}
		}
		
		var handleError = handleConnectionEvent.bind(undefined, 'error');
		var handleClose = handleConnectionEvent.bind(undefined, 'close');
		
		connection.on('error', handleError);
		connection.on('close', handleClose);
	}, function connectionFailed(error) {
		state.connecting = false;
		self._log(error);
		//TODO: Replace this ugly setTimeout with a real configurable delay strategy...
		state.reconnectTimeout = setTimeout(function() {
			self._createConnection();
		}, 5000);
	});
};

AMQPConnector.prototype._destroyConnection = function _destroyConnection() {
	if (this._state.connection) {
		this._state.destroy();
		this._state.connection = null;
	}
	clearTimeout(this._state.reconnectTimeout);
};

/**
 * Start the connector. This initiates the first connection attempt immediately after it is called.
 * Thus, start() should only be called after all interested callbacks have been registered (e.g. by using on()).
 * Multiple subsequent calls to this method have no effect - it is idempotent.
 * @fires module:AMQPBase.AMQPConnector#connect
 */
AMQPConnector.prototype.start = function start() {
	if (!this._state.started) {
		this._state.started = true;
		this._createConnection();
	}
};

/**
 * Stop the connector. This closes the active connection, if any, and aborts any outstanding connection attempts.
 * Stopping a connector which is already stopped is a no-op.
 * @fires module:AMQPBase.AMQPConnector#disconnect
 */
AMQPConnector.prototype.stop = function stop() {
	if (this._state.started) {
		this._state.started = false;
		this._destroyConnection();
	}
};

module.exports = AMQPConnector;
