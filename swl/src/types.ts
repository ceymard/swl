
export type TopologyTerminal = 'bool' | 'int' | 'number' | 'date' | 'blob' | 'string'
export type TopologyObj = TopologyTerminal | {[name: string]: Topology}

export type Topology =
  TopologyObj | TopologyObj[]

/**
 * Records are the objects that flow within our system
 */
export class Chunk {
  constructor(
    public collection: string,
    public topology: Topology,
    public data: any
  ) {

  }
}


/**
 * Events flow alongside data in the pipes.
 */
export class Event {
  constructor(
    public type: string
  ) {

  }
}
