
export type ChunkType =
'start'
| 'data'
| 'exec'

export interface ChunkBase {
  type: ChunkType
}

export interface StartChunk extends ChunkBase {
  type: 'start'
  name: string
}

export interface DataChunk extends ChunkBase {
  type: 'data'
  payload: any
}

export interface ExecChunk extends ChunkBase {
  type: 'exec'
  method: string
  options: any
  body: string
}

export type Chunk = StartChunk | DataChunk | ExecChunk

export namespace Chunk {
  export function data(payload: any): DataChunk {
    return {type: 'data', payload}
  }

  export function start(name: string): StartChunk {
    return {type: 'start', name}
  }
}

export type ChunkIterator = AsyncIterableIterator<Chunk>

export type Handler = (upstream: ChunkIterator) => ChunkIterator

export type Factory = (options: any, rest: string) => Handler


export function build_pipeline(handlers: Handler[]) {
  // Connect the handlers between themselves

}


/**
 * Conditional pipeline. If the condition is met, then the value travels down
 * another subpipeline
 */
export function condition(fn: (a: any) => any, pipeline: ChunkIterator) {
  return async function *condition(upstream: ChunkIterator): ChunkIterator {
    for await (var chk of upstream) {
      if (fn(chk)) {
        const res = await pipeline.next(chk)
        if (res.done)
          return
        yield res.value
      } else {
        yield chk
      }
    }
  }
}


export class FactoryContainer {
  registry = {} as {[name: string]: Factory}

  add(factory: Factory, ...mimes: string[]) {
    for (var name of [factory.name, ...mimes]) {
      this.registry[name] = factory
    }
  }

  get(name: string, options: any, rest: string) {

  }
}


export const sources = new FactoryContainer()
export const sinks = new FactoryContainer()