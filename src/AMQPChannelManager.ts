import { AsyncEventEmitter } from "./AsyncEventEmitter";
import { Channel, ConfirmChannel, Connection } from "amqplib";
import { EmitterOf } from "./EmitterOf";

interface AMQPChannelManagerOptions {
    /**
     * Whether the created channels should be put in confirm mode. Iff true, the #publish method of the emitted channels accepts an additional argument - a Node-style callback which executes when the message has been published.
     */
    confirm?: boolean;
};

type DestroyCallback = () => void;
type LogCallback = (occurrence: any) => void;
type ChannelWithOrWithoutConfirms = (Channel | ConfirmChannel);

type AMQPChannelManagerEmitter<ChannelType extends ChannelWithOrWithoutConfirms> =
    EmitterOf<"create",[ channel: ChannelType ]> &
    EmitterOf<"close",[ channel: ChannelType ]>;

interface IAMQPChannelManager<ChannelType extends ChannelWithOrWithoutConfirms> extends AMQPChannelManagerEmitter<ChannelType> {
    start(): void;
    stop(): void;
}

export type IAMQPChannelManagerNoConfirms = IAMQPChannelManager<Channel>;
export type IAMQPChannelManagerWithConfirms = IAMQPChannelManager<ConfirmChannel>;

/**
 * An AMQP Channel Manager is a state machine which uses a single AMQP connection to maintain a single channel open.
 * It automatically re-creates the channel in case it is dropped, unless the connection has also encountered an error.
 */
class AMQPChannelManager extends AsyncEventEmitter implements IAMQPChannelManager<ChannelWithOrWithoutConfirms> {
    protected _connection: Connection;
    protected _connectionClosed: boolean;
    protected _started: boolean;
    protected _creatingChannel: boolean;
    protected _channel: ChannelWithOrWithoutConfirms | null;
    protected _destroy: DestroyCallback | null;
    protected _options: AMQPChannelManagerOptions;
    protected _log: LogCallback;
    protected _recreateTimeout?: NodeJS.Timeout;

    /**
     * @param {external:AMQPConnection} connection An established AMQP connection, such as one obtained from an AMQPConnector's "connect" event.
     * @param {Object} [options] Settings for altering the channel manager's behaviour.
     */
    protected constructor(connection: Connection, options?: AMQPChannelManagerOptions) {
        super();
        this._connection = connection;
        this._connectionClosed = false;
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
        const self = this;
        connection.once('close', function() {
            self._destroyChannel();
            self._connectionClosed = true;
        });
        connection.once('error', function() {
            self._destroyChannel();
            self._connectionClosed = true;
        });
    }

    protected _getChannelPromise(): Promise<ChannelWithOrWithoutConfirms> {
        return this._options.confirm ?
            this._connection.createConfirmChannel() :
            this._connection.createChannel();
    }

    protected async _createChannel() {
        const self = this;
        const connection = self._connection;

        // If we're not started, or we already are in the middle of creating a channel, prevent the operation from taking place:
        if (!self._started || self._creatingChannel) {
            return;
        }

        self._creatingChannel = true;
        const channelPromise = this._getChannelPromise();
        
        try {
            const channel: ChannelWithOrWithoutConfirms = await channelPromise;
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

            function handleChannelEvent(closeType: string, eventData: any) {
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
        } catch (error) {
            self._creatingChannel = false;
            self._log(error);
            self._recreateTimeout = setTimeout(function() {
                self._createChannel();
            }, 5000);
        }
    }

    _destroyChannel() {
        if (this._channel) {
            this._destroy!();
            this._channel = null;
        }
        if (typeof this._recreateTimeout !== 'undefined') {
            clearTimeout(this._recreateTimeout);
        }
    }

    /**
     * Start maintaining an open channel. This requires that the connection still be open.
     */
    start() {
        if (this._connectionClosed) {
            throw new Error('The connection has been closed - can not start an AQMPChannelManager on a closed connection!');
        }
        if (!this._started) {
            this._started = true;
            this._createChannel();
        }
    }

    /**
     * Stop maintaining an open channel and, if already opened, close it.
     */
    stop() {
        if (this._started) {
            this._started = false;
            this._destroyChannel();
        }
    }

    static noConfirms(connection: Connection): IAMQPChannelManagerNoConfirms {
        return new AMQPChannelManagerNoConfirms(connection);
    }

    static withConfirms(connection: Connection): IAMQPChannelManagerWithConfirms {
        return new AMQPChannelManagerWithConfirms(connection);
    }    
}

class AMQPChannelManagerNoConfirms extends AMQPChannelManager implements IAMQPChannelManagerNoConfirms {
    protected _getChannelPromise(): Promise<Channel> {
        return this._connection.createChannel();
    }
}

class AMQPChannelManagerWithConfirms extends AMQPChannelManager implements IAMQPChannelManagerWithConfirms {
    protected _getChannelPromise(): Promise<ConfirmChannel> {
        return this._connection.createConfirmChannel();
    }
}

export { AMQPChannelManager };
