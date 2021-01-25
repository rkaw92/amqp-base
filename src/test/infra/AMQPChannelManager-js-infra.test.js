'use strict';

const AMQPConnector = require('../../AMQPConnector').AMQPConnector;
const AMQPChannelManager = require('../../AMQPChannelManager').AMQPChannelManager;
const uuid = require('uuid');

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@localhost:5676/%2f';

describe('AMQPChannelManager', function() {
    let connector;
    before(function() {
        connector = new AMQPConnector(AMQP_URL);
    });
    describe('#start', function() {
        it('should create a no-confirm channel in plain JS using the constructor directly', async function() {
            connector.start();
            await new Promise(function(resolve) {
                connector.once('connect', function(connection) {
                    const channelManager = new AMQPChannelManager(connection);
                    channelManager.start();
                    channelManager.on('create', function(channel) {
                        channel.publish('', 'test-' + uuid.v4(), Buffer.from('hello, world - emitted from amqp-base AMQPChannelManager-infra plain JS test 1'));
                        resolve();
                    });
                });
            });
            connector.stop();
        });
    });
});
