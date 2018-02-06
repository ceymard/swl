
export type TopologyTerminal = 'bool' | 'int' | 'number' | 'date' | 'blob' | 'string'

export type Topology =
  TopologyTerminal | {[name: string]: Topology}

/**
 * Records are the objects that flow within our system
 */
export interface Record {
  collection: string
  topology: Topology
  fields: any
}

export interface Event {

}