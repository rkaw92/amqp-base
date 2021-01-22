'use strict';

import EventEmitter from "events";

export class AsyncEventEmitter extends EventEmitter {
    /**
     * Emit an event in the next tick. This is the same as .emit(), but separates
     *  the event emission from the currently-executing function frame.
     * @param {string} eventName - Name of the event to emit.
     * @param {...*} arg - Argument for the emitted event. Can pass 0 or more arguments.
     */
    emitAsync(eventName: string, ...args: any[]) {
        const self = this;
        setImmediate(function() {
            self.emit.apply(self, [ eventName, ...args ]);
        });
    }
};
