import { AMQPConnector, IAMQPConnector } from "../../AMQPConnector";
import * as assert from "assert";

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@localhost:5676/%2f';

describe('AMQPConnector', function() {
    describe('#start', function() {
        it('should connect to a real AMQP broker', async function() {
            const connector: IAMQPConnector = new AMQPConnector(AMQP_URL);
            connector.start();
            const props: any = await new Promise(function(resolve) {
                connector.once('connect', function(connection) {
                    resolve(connection.connection.serverProperties);
                });
            });
            assert.strictEqual(typeof props.capabilities, 'object');
            assert.ok(/^[0-9]+\.[0-9]+\.[0-9]+/.test(props.version));
            connector.stop();
        });
    });
});
