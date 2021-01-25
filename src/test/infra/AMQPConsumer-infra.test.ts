import { AMQPConnector, IAMQPConnector } from "../../AMQPConnector";
import * as assert from "assert";
import { AMQPChannelManager, IAMQPChannelManagerNoConfirms, IAMQPChannelManagerWithConfirms } from "../../AMQPChannelManager";
import * as uuid from 'uuid';
import { AMQPConsumer, IAMQPConsumer } from "../../AMQPConsumer";
import { Channel } from "amqplib";

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@localhost:5676/%2f';

describe('AMQPConsumer', function() {
    let connector: IAMQPConnector;
    const exchangeName = 'TestExchange-' + uuid.v4();
    const queueName = 'testqueue-' + uuid.v4();
    const topic = 'testTopic';
    before(function() {
        connector = new AMQPConnector(AMQP_URL);
    });

    describe('#consume', function() {
        it('should allow one to receive messages from a queue', async function() {
            const testMessageBody = `Test message ${uuid.v4()}`;
            connector.start();
            await new Promise<void>(function(resolve, reject) {
                connector.once('connect', function(connection) {
                    // We need 2 channels: 1 for the producer and another for the consumer. Initialize 2 managers
                    //  so that we can get the respective channels up.
                    const producerChannelManager = AMQPChannelManager.noConfirms(connection);
                    producerChannelManager.start();
                    // Need to wrap the producer channel in a promise so that publishing can wait
                    //  for the consumer to be ready.
                    const producerChannelPromise = new Promise<Channel>(function(gotChannel) {
                        producerChannelManager.on('create', gotChannel);
                    });

                    const consumerChannelManager = AMQPChannelManager.noConfirms(connection);
                    consumerChannelManager.start();
                    consumerChannelManager.on('create', async function(channel) {
                        const consumer: IAMQPConsumer = new AMQPConsumer(channel, queueName, {
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
                        await consumer.consume();
                        consumer.on('message', function(message, ops) {
                            if (message.content.toString() === testMessageBody) {
                                ops.ack();
                                resolve();
                            } else {
                                ops.reject();
                                reject(new Error('Incorrect message received through consumer: ' + message.content.toString()));
                            }
                        });
                        // We now know the consumer is ready, and can publish the message.
                        const producerChannel = await producerChannelPromise;
                        producerChannel.publish(exchangeName, topic, Buffer.from(testMessageBody));
                    });
                });
            });
            connector.stop();
        });
    });
});
