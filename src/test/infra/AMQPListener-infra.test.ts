import { AMQPConnector, IAMQPConnector } from "../../AMQPConnector";
import * as assert from "assert";
import { AMQPChannelManager } from "../../AMQPChannelManager";
import * as uuid from 'uuid';
import { AMQPConsumer } from "../../AMQPConsumer";
import { Channel } from "amqplib";
import { AMQPListener } from "../../AMQPListener";

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@localhost:5676/%2f';

describe('AMQPListener', function() {
    let connector: IAMQPConnector;
    const exchangeName = 'TestExchange-' + uuid.v4();
    const queueName = 'testqueue-' + uuid.v4();
    const topic = 'testTopic';
    before(function() {
        connector = new AMQPConnector(AMQP_URL);
    });

    describe('#listen', function() {
        it('should enable receiving messages without constructing a channel manually', async function() {
            const testMessageBody = `Test message ${uuid.v4()}`;
            connector.start();
            await new Promise<void>(function(resolve, reject) {
                connector.once('connect', async function(connection) {
                    // Create a channel for the producer. The consumer's channel will be handled automatically
                    //  by the AMQPListener machinery, unlike in the AMQPConsumer test, which is lower-level.
                    const producerChannelManager = AMQPChannelManager.noConfirms(connection);
                    producerChannelManager.start();
                    // Need to wrap the producer channel in a promise so that publishing can wait
                    //  for the listener to initialize the consumer.
                    const producerChannelPromise = new Promise<Channel>(function(gotChannel) {
                        producerChannelManager.on('create', gotChannel);
                    });

                    const listener = new AMQPListener(connection, [ function(channel) {
                        const consumer = new AMQPConsumer(channel, queueName, {
                            queue: {
                                durable: false,
                                exclusive: true,
                                autoDelete: true
                            },
                            exchanges: [{
                                type: 'topic',
                                name: exchangeName,
                                options: {
                                    durable: false,
                                    autoDelete: true
                                }
                            }],
                            consume: {
                                exclusive: true,
                                prefetch: 0
                            },
                            binds: [{
                                exchange: exchangeName,
                                pattern: topic
                            }]
                        });
                        consumer.on('message', function(message, ops) {
                            if (message.content.toString() === testMessageBody) {
                                ops.ack();
                                resolve();
                            } else {
                                ops.reject();
                                reject(new Error('Incorrect message received through consumer: ' + message.content.toString()));
                            }
                        });
                        return consumer;
                    } ]);
                    await listener.listen();
                    const producerChannel = await producerChannelPromise;
                    producerChannel.publish(exchangeName, topic, Buffer.from(testMessageBody));
                });
            });
            connector.stop();
        });
    });
});
