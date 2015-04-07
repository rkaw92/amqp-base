var AMQPBase = require('../index');
var AMQPConnector = AMQPBase.AMQPConnector;
var AMQPConsumer = AMQPBase.AMQPConsumer;
var AMQPListener = AMQPBase.AMQPListener;

var connector = new AMQPConnector('amqp://test:Being7nymph$Clever@rabbitmq-dev//test');

connector.start();
connector.on('connect', function(connection) {
	console.log('* Connected');
		var consumerConstructor = function(channel) {
			var consumer = new AMQPConsumer(channel, 'TestConsumer', {
				exchanges: [ { name: 'DomainEvents', type: 'topic', options: { durable: true } } ],
				binds: [ { exchange: 'DomainEvents', pattern: '#' } ]
			});
			consumer.on('message', function(message, operations) {
				console.log('* message:', message);
				setTimeout(operations.requeue, 1000);
			});
			return consumer;
		};
		
		var listener = new AMQPListener(connection, [ consumerConstructor ]);
		
		listener.listen().done(function() {
			console.log('* Listening');
		});
});
