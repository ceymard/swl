import { Lock } from './streams'
import { Parser } from 'parsimmon'
import { Fragment, ADAPTER_AND_OPTIONS } from './cmdparse'

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

export type AsyncFactory<T, U> = (options: T, parsed: U) => Promise<Handler | Handler[]>
export type SyncFactory<T, U> = (options: T, parsed: U) => Handler | Handler[]
export type Factory<T, U> = SyncFactory<T, U> | AsyncFactory<T, U>


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
  const pipeline = instantiate_pipeline(handlers)

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


export type Unpromise<T> =
  T extends Promise<infer A> ? A
  : T

export type Un<T> =
  T extends [infer A, infer B, infer C] ? [Unpromise<A>, Unpromise<B>, Unpromise<C>]
  : T extends [infer A, infer B] ? [Unpromise<A>, Unpromise<B>]
  : Unpromise<T>


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
  add<T, U>(help: string, schema: y.ObjectSchema<T>, parser: Parser<U>, factory: Factory<T, Un<U>>, ...mimes: string[]): void
  add<T>(help: string, schema: y.ObjectSchema<T>, parser: null, factory: Factory<T, string>, ...mimes: string[]): void
  add<T, U>(help: string, schema: y.ObjectSchema<T>, parser: null | Parser<U>, factory: Factory<T, U>, ...mimes: string[]) {
    const factory_object = {help, factory, schema, parser, mimes}
    this.all.push(factory_object)
    for (var name of mimes) {
      this.map[name] = factory_object
    }
  }

  [Symbol.iterator]() {
    this.all.sort((a, b) => a.mimes[0] < b.mimes[0] ? -1 :
      a.mimes[0] > b.mimes[0] ? 1 : 0)
    return this.all[Symbol.iterator]()
  }

  /**
   * Get a handler
   * @param name The name, extension or mimetype of the handler
   * @param options Provided options to the factory
   * @param rest The rest of the command line string
   */
  async get(name: string, options: any, rest: string): Promise<Handler | Handler[] | null> {
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
      rest = rest ? name + ' ' + rest : name
    }

    if (!handler) return null

    var opts = handler.schema.cast(options)
    var parsed = await (handler.parser ? handler.parser.tryParse(rest) : rest)

    if (Array.isArray(parsed)) {
      parsed = await (Promise.all(parsed))
    }

    return await handler.factory(opts, parsed)
  }
}


export const sources = new FactoryContainer()
export const sinks = new FactoryContainer()
export const transformers = new FactoryContainer()


export function instantiate_pipeline(handlers: Handler[]) {

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


export async function build_pipeline(fragments: Fragment[]) {
  const pipe = [] as Handler[]
  for (var f of fragments) {
    var [_name, opts, rest] = ADAPTER_AND_OPTIONS.tryParse(f.inst)
    const name = await _name

    var handler = f.type === 'source' ?
      await sources.get(name, opts, rest) : await transformers.get(name, opts, rest) || await sinks.get(name, opts, rest)

    if (Array.isArray(handler)) {
      for (var h of handler) pipe.push(h)
    } else if (handler) {
      pipe.push(handler)
    } else {
      throw new Error(`No handler for '${name}'`)
    }
  }
  return pipe
}


