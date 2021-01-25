import { AsyncEventEmitter } from "../../AsyncEventEmitter";
import * as assert from "assert";

describe('AsyncEventEmitter', function() {
    it('should not emit the event right away', function() {
        const emitter = new AsyncEventEmitter();
        let called = false;
        emitter.on('testevent', function() {
            called = true;
        });
        emitter.emitAsync('testevent');
        // NOTE: We don't wait for the next tick here, so it should remain false for now.
        assert.strictEqual(called, false);
    });
    it('should emit the event in the future', async function() {
        const emitter = new AsyncEventEmitter();
        let called = false;
        emitter.on('testevent', function() {
            called = true;
        });
        emitter.emitAsync('testevent');
        await new Promise(function(resolve) {
            setTimeout(resolve, 20);
        });
        assert.strictEqual(called, true);
    });
});