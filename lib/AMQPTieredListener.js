/**
 * @module AMQPTieredListener
 */

var AMQPListener = require('./AMQPListener');
var AMQPConsumer = require('./AMQPConsumer');
var when = require('when');

/**
 * A Tiered Listener is specialization of the Listener. It uses multiple consumers/queues (called tiers) to implement a hierarchical delayed retry policy.
 * A tier is a queue with specific consumption behaviour with regard to waiting after a failure.
 * By nature, AMQP consumers always run at full throughput - that is, messages are sent as soon as possible, and processing is not delayed.
 * However, for some problematic messages, it is desirable to delay retrying so as to ease the load on the system.
 * This is implemented using tiers (queues), where every but the last tier has a "dead letter exchange" set, so that rejected messages
 *  fall through to the next queue.
 */
function AMQPTieredListener(connection, queueNameBase, tiers, options) {
	options = options ? Object.keys(options).reduce(function shallowCopyOption(copiedOptions, optionName) {
		copiedOptions[optionName] = options[optionName];
		return copiedOptions;
	}, {}) : {};
	options.queue = options.queue || {};
	options.consume = options.consume || {};
	options.exchanges = options.exchanges || [];
	options.binds = options.binds || [];
	options.routing = options.routing || {};
		
	var messageHandler = options.messageHandler || function handleMessage(message) {
		console.warn('Warning: no message handler set. Rejecting...');
		return when.reject(new Error('No message handler set for the tiered listener'));
	};
	
	var deadLetterExchange = options.routing.deadLetterExchange || {
		name: queueNameBase + 'DLX',
		type: 'direct',
		options: { durable: true }
	};
	
	var circular = options.routing.circular || false;
	
	// Prevent any outside changes from interfering with our array structure:
	tiers = tiers.slice();
	
	var exchanges = options.exchanges.concat([ deadLetterExchange ]);
	
	var consumerConstructors = tiers.map(function createTierQueueConstructor(tier, tierIndex) {
		var tierQueueName = queueNameBase + '-' + tier.name;
		
		return function constructConsumer(channel) {
			// First, copy over the options originally supplied to the constructor:
			var queueOptions = {};
			Object.keys(options.queue).forEach(function copyOption(optionName) {
				queueOptions[optionName] = options.queue[optionName];
			});
			
			// Now, extend the object using a dead letter exchange and routing key, unless this is the last tier:
			if (tierIndex !== tiers.length - 1) {
				queueOptions.deadLetterExchange = deadLetterExchange.name;
				queueOptions.deadLetterRoutingKey = tiers[tierIndex + 1].name;
			}
			// However, if this is the last tier, and the user has requested circular inter-tier routing (i.e. last falls through to first), apply it:
			if ((tierIndex === tiers.length - 1) && circular) {
				queueOptions.deadLetterExchange = deadLetterExchange.name;
				queueOptions.deadLetterRoutingKey = tiers[0].name;
			}
			
			// We need to bind the queue to the dead letter exchange:
			var queueBinds = [{
				exchange: deadLetterExchange.name,
				pattern: tier.name
			}];
			// In addition, the first tier is also bound to the nominal exchange(s) as requested by the user:
			if (tierIndex === 0) {
				queueBinds = queueBinds.concat(options.binds);
			}
				
			// Next, construct a complete options object using it:
			var singleConsumerOptions = {
				queue: queueOptions,
				consume: options.consume,
				exchanges: exchanges,
				binds: queueBinds
			};
			
			var consumer = new AMQPConsumer(channel, tierQueueName, singleConsumerOptions);
			consumer.on('message', function(message, operations) {
				when.try(messageHandler, message).done(function messageProcessed() {
					operations.ack();
				}, function messageProcessingFailed(reason) {
					//TODO: Log the failure somehow, if requested by the caller.
					// If this is not the last tier, we can reject the message so that it falls through to the next one:
					if ((tierIndex !== tiers.length - 1) || circular) {
						setTimeout(operations.reject, tier.delay);
					}
					else {
						// This is the last tier. We must not lose the message!
						setTimeout(operations.requeue, tier.delay);
					}
				});
			});
			return consumer;
		};
	});
	this._listener = new AMQPListener(connection, consumerConstructors);
	
	return this._listener;
}

module.exports = AMQPTieredListener;