export declare type TopologyTerminal = 'bool' | 'int' | 'number' | 'date' | 'blob' | 'string';
export declare type TopologyObj = TopologyTerminal | {
    [name: string]: Topology;
};
export declare type Topology = TopologyObj | TopologyObj[];
/**
 * Records are the objects that flow within our system
 */
export declare class Data {
    collection: string;
    topology: Topology;
    data: any;
    constructor(collection: string, topology: Topology, data: any);
}
/**
 * Events flow alongside data in the pipes.
 */
export declare class Event {
    type: string;
    constructor(type: string);
}
