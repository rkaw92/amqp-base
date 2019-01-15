'use strict';

var EventEmitter = require('events').EventEmitter;

function AsyncEventEmitter(options) {
	EventEmitter.call(this, options);
}
AsyncEventEmitter.prototype = Object.create(EventEmitter.prototype);

/**
 * Emit an event in the next tick. This is the same as .emit(), but separates
 *  the event emission from the currently-executing function frame.
 * @param {string} eventName - Name of the event to emit.
 * @param {...*} arg - Argument for the emitted event. Can pass 0 or more arguments.
 */
AsyncEventEmitter.prototype.emitAsync = function emitAsync() {
	var self = this;
	var emitArgs = arguments;
	setImmediate(function() {
		self.emit.apply(self, emitArgs);
	});
};

module.exports = AsyncEventEmitter;
