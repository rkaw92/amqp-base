/*
* This example creates a Tiered Listener, which will pass a message between multiple tiers (queues) of processing upon failure.
* Processing starts by routing the message from the exchange to the first tier's queue, where it is picked up by our listener.
* If the first tier fails to process the message, it is moved to the second one.
* The tiers differ in the delay after a failed processing operation that they will apply, before handing the message off to the next tier.
* The last tier never hands the message off to another, unless routing.circular is set. Thus, the last tier is the final destination
*  for all messages in the pessimistic case.
*/

var AMQPBase = require('../index');
var AMQPConnector = AMQPBase.AMQPConnector;
var AMQPConsumer = AMQPBase.AMQPConsumer;
var AMQPTieredListener = AMQPBase.AMQPTieredListener;

var connector = new AMQPConnector('amqp://test:Being7nymph$Clever@rabbitmq-dev//test');

connector.start();
connector.on('connect', function(connection) {
		console.log('* Connected');
		
		var listener = new AMQPTieredListener(connection, 'processor', [{
			name: 'fast',
			delay: 500
		}, {
			name: 'medium',
			delay: 2000,
		}, {
			name: 'slow',
			delay: 10000
		}], {
			exchanges: [{
				name: 'Events',
				type: 'topic',
				options: { durable: true }
			}],
			binds: [{
				exchange: 'Events',
				pattern: '#'
			}],
			consume: {
				prefetch: 10
			},
			messageHandler: function handleMessage(message) {
				// Note: this handler could also return any value instead of throwing, so that messages would be ACKed properly.
				console.log('Borking message:', message);
				throw new Error('Artificial failure!');
			},
			routing: {
				circular: false
			}
		});
		listener.listen().done(function() {
			console.log('* Listening');
		});
});
