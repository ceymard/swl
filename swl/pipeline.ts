
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

export type AsyncFactory<T> = (options: T, rest: string) => Promise<Handler>
export type SyncFactory<T> = (options: T, rest: string) => Handler
export type Factory<T> = SyncFactory<T> | AsyncFactory<T>


export async function build_pipeline(handlers: Handler[]) {

  async function *start_generator(): ChunkIterator {
    return
  }
  // Connect the handlers between themselves
  // console.log('toto ?', handlers[0].name)
  var handler = handlers[0](start_generator())
  for (var h of handlers.slice(1)) {
    handler = h(handler)
  }

  for await (var ch of handler) {
    ch // useless.
  }
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


import * as p from 'path'
import { start } from 'repl';
import * as y from 'yup'

export class FactoryContainer {
  registry = {} as {[name: string]: {factory: Factory<any>, schema: y.ObjectSchema<any>} | undefined}

  add<T>(schema: y.ObjectSchema<T>, factory: Factory<T>, ...mimes: string[]) {
    for (var name of [factory.name, ...mimes]) {
      this.registry[name] = {factory, schema}
    }
  }

  async get(name: string, options: any, rest: string) {
    var handler = this.registry[name]

    if (!handler) {
      const a = p.parse(name)
      handler = this.registry[a.ext]
      rest = `${name} ${rest}`
    }

    if (!handler) throw new Error(`No handler for ${name}`)

    var opts = handler.schema.cast(options)
    return await handler.factory(opts, rest)
  }
}


export const sources = new FactoryContainer()
export const sinks = new FactoryContainer()
