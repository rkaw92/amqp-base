/**
 * This example demonstrates basic usage of the AMQPPublishStream, which implements a stream that message objects can be written to for publishing.
 */

var AMQPBase = require('../index');
var AMQPConnector = AMQPBase.AMQPConnector;
var AMQPChannelManager = AMQPBase.AMQPChannelManager;
var AMQPPublishStream = AMQPBase.AMQPPublishStream;

var connector = new AMQPConnector('amqp://test:Being7nymph$Clever@rabbitmq-dev//test');

connector.start();
connector.on('connect', function(connection) {
	console.log('* Connected');
	// Create a channel manager and tell it that we are going to need Confirm Channels:
	var confirmChannelManager = new AMQPChannelManager(connection, { confirm: true });
	
	confirmChannelManager.on('create', function(channel) {
		console.log('* Channel obtained');
		// Now that we have got a channel, make sure than an exchange is present so that we can publish to it.
		//TODO: The publisher stream should support this feature using exchange declarations instead of forcing us to use amqplib's API directly.
		channel.assertExchange('publish-stream-example', 'fanout', {
			durable: false
		}).done(function() {
			console.log('* Exchange asserted');
			// Create a PublishStream, which will let us write messages to AMQP:
			var publishStream = new AMQPPublishStream(channel);
			
			var periodicWriter = setInterval(function() {
				// At every write, we need to tell what exchange this message should go to and what routing key it should be sent with.
				publishStream.write({
					exchange: 'publish-stream-example',
					routingKey: 'example',
					content: 'This is an example confirmed message.',
					options: { expiration: 5000 }
				}, function(error) {
					// Since the WritableStream API is callback-based by Node conventions, we need to supply a callback here. An alternative would be to use nodefn.call from when.js.
					if (error) {
						return void console.error('write() error:', error);
					}
					console.log('Message written and confirmed.');
				});
			}, 5000);
			
			// Pay special attention to this 'error' event handler. It is within the typical behaviour of Writable Streams
			//  to emit an "error" event whenever a write error occurs, especially after the first write error.
			// The following handler can be registered using .once(), because it prevents further writes from happening (clearInterval).
			
			// Try commenting it out and then launching this example script. If you subsequently remove the 'publish-stream-example' exchange,
			//  the channel will be closed (due to a channel error - publishing to a non-existent exchange), and write errors will be emitted,
			//  thus destroying the whole Node process.
			publishStream.once('error', function(error) {
				console.error('publishStream error event:', error);
				clearInterval(periodicWriter);
			});
		});
	});
	
	confirmChannelManager.start();
});
