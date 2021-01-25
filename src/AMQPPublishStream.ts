import { ConfirmChannel } from "amqplib";
import { Writable as WritableStream } from "stream";

export interface PublishCallback {
    (): void;
};

export interface Message {
    exchange: string;
    routingKey: string;
    content: string | Buffer;
    options?: {
        persistent?: boolean;
    };
    callback?: PublishCallback;
};

export interface AMQPPublishStreamOptions {
    highWaterMark?: number;
};

/**
 * General interface that's suitable for integration with producer streams without too much hassle about types.
 * It mirrors Node.js' Writable stream interface 1:1, meaning TypeScript will not detect obvious mistakes like
 *  writing strings, Buffers or malformed objects using write() (or pipe() etc.).
 */
export interface IAMQPPublishStream extends WritableStream {};

/**
 * Strict interface that's safer to use as it will only accept well-formed Message objects type-wise.
 * Mostly useful when constructing Message objects and passing them directly using .write().
 */
export interface IAMQPPublishStreamStrict extends Omit<IAMQPPublishStream,"write"> {
    write(chunk: Message, cb?: (error: Error | null | undefined) => void): boolean;
    write(chunk: Message, encoding: BufferEncoding, cb?: (error: Error | null | undefined) => void): boolean;
};

/**
 * A Writable Stream that allows publishing messages to the AMQP bus.
 * Calls the write callback only when the broker acknowledges them, ensuring durability.
 */
class AMQPPublishStream extends WritableStream implements IAMQPPublishStream, IAMQPPublishStreamStrict {
    private _channel: ConfirmChannel;
    private _options: AMQPPublishStreamOptions;
    /**
     * Construct a writable stream for publishing to AMQP.
     * @param {external:AMQPChannel} channel An AMQP channel that communication with the broker shall be carried out over. The channel should be a Confirm Channel, unless confirmation-less operation is explicitly requested via options.
     * @param {Object} [options] Additional options, controlling the behaviour of the stream.
     * @param {number} [options.highWatermark=8] The number of outstanding messages that have not yet received a confirmation after which #write will start returning false to throttle writers to this stream.
     */
    constructor(channel: ConfirmChannel, options?: AMQPPublishStreamOptions) {
        options = options || {};
        super({
            objectMode: true,
            highWaterMark: options.highWaterMark || 8,
            decodeStrings: true
        });
        this._channel = channel;
        this._options = options;
    }

     _write(message: Message, _encoding: any, writeCallback: (error?: Error | null) => void) {
        var exchange: Message['exchange'];
        var routingKey: Message['routingKey'];
        var content: Buffer;
        var options: Message['options'];
        var customPublishCallback: PublishCallback | null;
        
        // First, make sure that the message has all the fields that we expect of it:
        try {
            if (!message || typeof(message) !== 'object') {
                throw new Error('Messages to publish must be objects');
            }
            exchange = message.exchange || '';
            routingKey = message.routingKey;
            content = Buffer.isBuffer(message.content) ? message.content : Buffer.from(message.content, 'utf-8');
            options = message.options || { persistent: false };
            customPublishCallback = message.callback || null;
            
            if (!routingKey) {
                throw new Error('No routing key provided in the message');
            }
        }
        catch (messageVerificationError) {
            return void writeCallback(messageVerificationError);
        }
        
        try {
            this._channel.publish(exchange, routingKey, content, options, function publishCallback(publishError) {
                if (publishError) {
                    return void writeCallback(publishError || new Error('Undefined error received via publisher confirm callback - bailing out just in case'));
                }
                
                writeCallback();
                if (customPublishCallback) {
                    customPublishCallback();
                }
            });
        } catch(publishError) {
            writeCallback(publishError || new Error('Undefined error while publishing message to the broker - bailing out just in case'));
        }
    }
}

export { AMQPPublishStream };
