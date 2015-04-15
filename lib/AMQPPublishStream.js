var WritableStream = require('stream').Writable;
var when = require('when');

/**
 * A single AMQP message that can be published.
 * @typedef {Object} module:AMQPBase.AMQPPublishStream~Message
 * @property {string} exchange The name of the exchange to which the message should be published. If empty or otherwise falsy, the default exchange (direct-to-queue) is used.
 * @property {string} routingKey The routing key that the message is to be published with. It is used by the broker when determining which queues to route the message to.
 * @property {(string|Buffer)} content Message payload. Will be delivered as-is to the recipient. Strings, if passed, are encoded using utf-8.
 * @property {Object} [options] Additional delivery options. Refer to {@link http://www.squaremobius.net/amqp.node/doc/channel_api.html|the channel API}, specifically #publish, for details.
 */


/**
 * A Writable Stream that allows publishing messages to the AMQP bus.
 * @constructor
 * @memberof module:AMQPBase
 * @extends external:WritableStream
 * 
 * @param {external:AMQPChannel} channel An AMQP channel that communication with the broker shall be carried out over. The channel should be a Confirm Channel, unless confirmation-less operation is explicitly requested via options.
 * @param {Object} [options] Additional options, controlling the behaviour of the stream.
 * @param {boolean} [options.confirm=true] Whether publisher confirms should be used. This requires the "channel" parameter to be a Confirm Channel (use AMQPChannelManager's "confirm" option to obtain such a channel).
 * @param {number} [options.highWatermark=8] The number of outstanding messages that have not yet received a confirmation after which #write will start returning false to throttle writers to this stream.
 */
function AMQPPublishStream(channel, options) {
	options = options || {};
	
	WritableStream.call(this, {
		objectMode: true,
		highWatermark: options.highWatermark || 8,
		decodeStrings: true
	});
	
	this._channel = channel;
	this._options = options;
}
AMQPPublishStream.prototype = Object.create(WritableStream.prototype);

/**
 * Publish a message. This is an internal method which is called by the WritableStream's back-pressure management code whenever actual writing is meant to take place.
 */
AMQPPublishStream.prototype._write = function _write(message, encoding, writeCallback) {
	var exchange;
	var routingKey;
	var content;
	var options;
	
	// First, make sure that the message has all the fields that we expect of it:
	try {
		if (!message || typeof(message) !== 'object') {
			throw new Error('Messages to publish must be objects');
		}
		exchange = message.exchange || '';
		routingKey = message.routingKey;
		content = new Buffer(message.content, 'utf-8');
		options = message.options || { persistent: false };
		
		if (!routingKey) {
			throw new Error('No routing key provided in the message');
		}
	}
	catch (messageVerificationError) {
		return void writeCallback(messageVerificationError);
	}
	
	try {
		//TODO: Implement conditional confirmation usage.
		this._channel.publish(exchange, routingKey, content, options, function publishCallback(publishError) {
			if (publishError) {
				return void writeCallback(publishError || new Error('Undefined error received via publisher confirm callback - bailing out just in case'));
			}
			
			writeCallback();
		});
	} catch(publishError) {
		writeCallback(publishError || new Error('Undefined error while publishing message to the broker - bailing out just in case'));
	}
};

module.exports = AMQPPublishStream;
