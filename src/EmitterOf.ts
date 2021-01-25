export interface EmitterOf<EventType extends string, ListenerArgs extends Array<any>> {
    on(event: EventType, listener: (...args: ListenerArgs) => void): void;
    once(event: EventType, listener: (...args: ListenerArgs) => void): void;
    off(event: EventType, listener: (...args: ListenerArgs) => void): void;
};
