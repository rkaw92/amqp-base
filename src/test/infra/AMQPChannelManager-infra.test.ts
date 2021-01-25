import { AMQPConnector, IAMQPConnector } from "../../AMQPConnector";
import * as assert from "assert";
import { AMQPChannelManager, IAMQPChannelManagerNoConfirms, IAMQPChannelManagerWithConfirms } from "../../AMQPChannelManager";
import * as uuid from 'uuid';

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@localhost:5676/%2f';

describe('AMQPChannelManager', function() {
    let connector: IAMQPConnector;
    before(function() {
        connector = new AMQPConnector(AMQP_URL);
    });

    describe('noConfirms', function() {
        describe('#start', function() {
            it('should create a no-confirm channel suitable for publishing', async function() {
                connector.start();
                await new Promise<void>(function(resolve) {
                    connector.once('connect', function(connection) {
                        const channelManager = AMQPChannelManager.noConfirms(connection);
                        channelManager.start();
                        channelManager.on('create', function(channel) {
                            // We're running without publisher confirms, so if publish doesn't throw immediately, we consider the job done.
                            channel.publish('', 'test-' + uuid.v4(), Buffer.from('hello, world - emitted from amqp-base AMQPChannelManager-infra test 2'));
                            resolve();
                        });
                    });
                });
                connector.stop();
            });
        });
    });

    describe('withConfirms', function() {
        describe('#start', function() {
            it('should create a no-confirm channel suitable for publishing', async function() {
                connector.start();
                await new Promise<void>(function(resolve, reject) {
                    connector.once('connect', function(connection) {
                        const channelManager = AMQPChannelManager.withConfirms(connection);
                        channelManager.start();
                        channelManager.on('create', function(channel) {
                            channel.publish('', 'test-' + uuid.v4(), Buffer.from('hello, world - emitted from amqp-base AMQPChannelManager-infra test 3'), undefined, function(err) {
                                // In this test, we wait for the publisher confirm.
                                if (err) {
                                    return void reject(err);
                                }
                                resolve();
                            });
                        });
                    });
                });
                connector.stop();
            });
        });
    });
});
