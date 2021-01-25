export type ExchangeType = "direct" | "topic" | "fanout" | "headers";

export interface Exchange {
    name: string;
    type: ExchangeType;
    options?: any;
};
