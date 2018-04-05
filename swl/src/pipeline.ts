import { Lock } from './streams'
import { Parser } from 'parsimmon'

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

export type AsyncFactory<T, U> = (options: T, parsed: U) => Promise<Handler>
export type SyncFactory<T, U> = (options: T, parsed: U) => Handler
export type Factory<T, U> = SyncFactory<T, U> | AsyncFactory<T, U>


export function build_pipeline(handlers: Handler[]) {

  async function *start_generator(): ChunkIterator {
    return
  }
  // Connect the handlers between themselves
  // console.log('toto ?', handlers[0].name)
  var handler = handlers[0](start_generator())
  for (var h of handlers.slice(1)) {
    handler = h(handler)
  }

  return handler
}


/**
 * Conditional pipeline. If the condition is met, then the value travels down
 * another subpipeline
 */
export function condition(fn: (a: any) => any, handlers: Handler[]) {

  const lock = new Lock<any>()
  const end = Symbol()
  async function *fake_source(): ChunkIterator {
    do {
      var res = await lock.promise
    } while (res !== end)
  }

  handlers.unshift(fake_source)
  const pipeline = build_pipeline(handlers)

  return async function *condition(upstream: ChunkIterator): ChunkIterator {
    for await (var chk of upstream) {
      if (fn(chk)) {
        // FIXME feed the pipeline
        lock.resolve(chk)
        const res = await pipeline.next()
        if (res.done)
          return
        yield res.value
      } else {
        yield chk
      }
    }
    // Once upstream is done, we send end down the pipeline
    // which should trigger the end of the fake_source and thus
    // trigger the possible sources that were inside this pipe.
    lock.resolve(end)
    // We now yield the rest of the pipeline
    yield* pipeline
  }
}


import * as p from 'path'
import * as y from 'yup'


export interface FactoryObject {
  help: string,
  factory: Factory<any, any>
  parser: Parser<any> | null
  schema: y.ObjectSchema<any>
  mimes: string[]
}


/**
 * We use this class to store named factories so that the command
 * parser find them.
*/
export class FactoryContainer {
  map = {} as {[name: string]: FactoryObject | undefined}
  all = [] as FactoryObject[]

  /**
   * Add a factory to the registry
   * @param schema A schema for the options that this factory accepts
   * @param factory The factory function
   * @param mimes Extensions or mime types that this handler accepts
   */
  add<T, U>(help: string, schema: y.ObjectSchema<T>, parser: Parser<U>, factory: Factory<T, U>, ...mimes: string[]): void
  add<T>(help: string, schema: y.ObjectSchema<T>, parser: null, factory: Factory<T, string>, ...mimes: string[]): void
  add<T, U>(help: string, schema: y.ObjectSchema<T>, parser: null | Parser<U>, factory: Factory<T, U>, ...mimes: string[]) {
    const factory_object = {help, factory, schema, parser, mimes}
    this.all.push(factory_object)
    for (var name of mimes) {
      this.map[name] = factory_object
    }
  }

  [Symbol.iterator]() {
    return this.all[Symbol.iterator]()
  }

  /**
   * Get a handler
   * @param name The name, extension or mimetype of the handler
   * @param options Provided options to the factory
   * @param rest The rest of the command line string
   */
  async get(name: string, options: any, rest: string) {
    var handler = this.map[name]

    if (!handler) {
      const re_uri = /^([\w]+):\/\//
      const match = re_uri.exec(name)
      if (match) {
        handler = this.map[match[1]]
        rest = `${name.replace(match[0], '')} ${rest}`
      }
    }

    if (!handler) {
      const a = p.parse(name)
      handler = this.map[a.ext]
      rest = `${name} ${rest}`
    }

    if (!handler) throw new Error(`No handler for ${name}`)

    var opts = handler.schema.cast(options)
    var parsed = handler.parser ? handler.parser.tryParse(rest) : rest
    return await handler.factory(opts, parsed)
  }
}


export const sources = new FactoryContainer()
export const sinks = new FactoryContainer()
