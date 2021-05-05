import { Connection } from "amqplib";
import { AsyncEventEmitter } from "./AsyncEventEmitter";
import * as amqplib from "amqplib";
import { EmitterOf } from "./EmitterOf";

type ServerURI = string;

export interface AMQPConnectorOptions {
    socket?: {
        noDelay?: boolean;
    }
};

type DestroyCallback = () => void;
type LogCallback = (occurrence: any) => void;

interface AMQPConnectorState {
    started: boolean;
    connecting: boolean;
    connection: Connection | null;
    lastUsedURI: ServerURI | null;
    reconnectTimeout: NodeJS.Timeout | null;
    destroy: DestroyCallback | null;
};

interface AMQPConnectorEmitter extends EmitterOf<"connect",[ connection: Connection ]> {}

export interface IAMQPConnector extends AMQPConnectorEmitter {
    start(): void;
    stop(): void;
}

class AMQPConnector extends AsyncEventEmitter implements IAMQPConnector {
    private _serverURIs: ServerURI[];
    private _options: AMQPConnectorOptions;
    private _state: AMQPConnectorState;
    private _log: LogCallback;

    constructor(serverURI: ServerURI | ServerURI[], options?: AMQPConnectorOptions) {
        super();
        this.setMaxListeners(0);

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

        this._log = function log(occurrence) {
            console.log('[%s] AMQPConnector:', (new Date()).toISOString(), occurrence);
        };

        const self = this;
        // Hand out existing connections if a listener registers after we've connected:
        this.on('newListener', function(event, listener) {
            if (event === 'connect' && self._state.connection) {
                listener(self._state.connection!);
            }
        });
    }

    private _selectServerURI(lastUsedURI: ServerURI | null) {
        const lastURIIndex = lastUsedURI ? this._serverURIs.indexOf(lastUsedURI) : -1;
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
    }

    private async _createConnection() {
        var self = this;
        var state = this._state;
    
        // Guard clause: if we are not started (yet/anymore), or already trying to connect, do nothing. This should end a reconnection chain and prevent multiple concurrent connections.
        if (!state.started || state.connecting) {
            return;
        }
    
        var URI = self._selectServerURI(state.lastUsedURI);
        state.lastUsedURI = URI;
        state.connecting = true;
        try {
            // TODO: Refactor: inject the amqplib instance to allow replacing with a test double.
            const connection = await amqplib.connect(URI, self._options.socket);
            connection.setMaxListeners(0);
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
            self.emitAsync('connect', connection);
    
            function handleConnectionEvent(closeType: string, eventData: any) {
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
                self.emitAsync('disconnect', connection);
    
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
        } catch (error) {
            state.connecting = false;
            self._log(error);
            //TODO: Replace this ugly setTimeout with a real configurable delay strategy...
            state.reconnectTimeout = setTimeout(function() {
                self._createConnection();
            }, 5000);
        }
    }

    private _destroyConnection() {
        // TODO: Refactor: Get rid of TypeScript not-null coercion (!), replace by proper states and state duck-type checks.
        if (this._state.connection) {
            this._state.destroy!();
            this._state.connection = null;
        }
        clearTimeout(this._state.reconnectTimeout!);
    }

    public start() {
        if (!this._state.started) {
            this._state.started = true;
            this._createConnection();
        }
    }

    public stop() {
        if (this._state.started) {
            this._state.started = false;
            this._destroyConnection();
        }
    }
}

export { AMQPConnector };
