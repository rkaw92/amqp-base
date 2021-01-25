import { AMQPConnector, IAMQPConnector } from "../../AMQPConnector";
import * as assert from "assert";
import { AMQPChannelManager, IAMQPChannelManagerNoConfirms, IAMQPChannelManagerWithConfirms } from "../../AMQPChannelManager";
import * as uuid from 'uuid';
import { AMQPConsumer, IAMQPConsumer } from "../../AMQPConsumer";
import { Channel, ConfirmChannel } from "amqplib";
import { AMQPPublishStream, IAMQPPublishStreamStrict } from "../../AMQPPublishStream";

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@localhost:5676/%2f';

describe('AMQPPublishStream', function() {
    let connector: IAMQPConnector;
    const exchangeName = 'TestExchange-' + uuid.v4();
    const queueName = 'testqueue-' + uuid.v4();
    const topic = 'testTopic';
    before(function() {
        connector = new AMQPConnector(AMQP_URL);
    });

    describe('#write', function() {
        it('should send message to bus', async function() {
            const testMessageBody = `Test message ${uuid.v4()}`;
            connector.start();
            await new Promise<void>(function(resolve, reject) {
                connector.once('connect', function(connection) {
                    // As usual, we'll be using a producer-consumer pair for testing. This is to see
                    //  if the messages really are being sent.
                    // Note that the Publish Stream only works with confirm channels, so we use
                    //  the withConfirms flavor of AMQPChannelManager to establish a channel.
                    const producerChannelManager = AMQPChannelManager.withConfirms(connection);
                    producerChannelManager.start();
                    const producerChannelPromise = new Promise<ConfirmChannel>(function(gotChannel) {
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
                        const writable: IAMQPPublishStreamStrict = new AMQPPublishStream(await producerChannelPromise);
                        writable.write({
                            exchange: exchangeName,
                            routingKey: topic,
                            content: testMessageBody
                        }, function(err) {
                            if (err) {
                                reject(err);
                            } else {
                                // no-op: the test does not end because we're waiting for the consumer to get our message.
                            }
                        });
                    });
                });
            });
            connector.stop();
        });
    });
});
