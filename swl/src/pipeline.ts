import { Lock } from './streams'
import { Parser } from 'parsimmon'
import { Fragment, ADAPTER_AND_OPTIONS } from './cmdparse'

export type ChunkType =
'start'
| 'data'
| 'exec'
| 'info'

export namespace Chunk {
  export interface ChunkBase {
    type: ChunkType
  }

  export interface Start extends ChunkBase {
    type: 'start'
    name: string
  }

  export interface Data extends ChunkBase {
    type: 'data'
    payload: any
  }

  export interface Exec extends ChunkBase {
    type: 'exec'
    method: string
    options: any
    body: string
  }

  export interface Info extends ChunkBase {
    type: 'info'
    level: number
    source: string
    message: string
    payload: any
  }

  export function data(payload: any): Data {
    return {type: 'data', payload}
  }

  export function start(name: string): Start {
    return {type: 'start', name}
  }

  export function info(source: string, message: string, payload?: any): Info {
    return {type: 'info', source, message, payload, level: 10}
  }

}


export type Chunk = Chunk.Start | Chunk.Data | Chunk.Exec | Chunk.Info


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
  map = {} as {[name: string]: new () => PipelineComponent<any, any> | undefined}
  all = [] as {component: (new () => PipelineComponent<any, any>), mimes: string[]}[]

  /**
   * Add a factory to the registry
   * @param schema A schema for the options that this factory accepts
   * @param factory The factory function
   * @param mimes Extensions or mime types that this handler accepts
   */
  add(mimes: string[], component: new () => PipelineComponent<any, any>) {
    this.all.push({component, mimes})
    for (var name of mimes) {
      this.map[name] = component
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
  async get(name: string, options: any, rest: string): Promise<PipelineComponent<any, any> | null> {
    var factory = this.map[name]

    if (!factory) {
      const re_uri = /^([\w]+):\/\//
      const match = re_uri.exec(name)
      if (match) {
        factory = this.map[match[1]]
        rest = `${name.replace(match[0], '')} ${rest}`
      }
    }

    if (!factory) {
      const a = p.parse(name)
      factory = this.map[a.ext]
      rest = rest ? name + ' ' + rest : name
    }

    if (!factory) return null

    var handler = new factory()!
    handler.options = handler.options_parser ? handler.options_parser.deserialize(options) : options
    const parser = handler.body_parser
    var parsed: any = rest
    if (parser) {
      const result = parser.parse(rest)
      if (result.status) {
        parsed = await result.value
      } else {

        // const offset = result.index.offset
        // const r = rest.slice(offset)
        // console.log(rest)
        throw new Error(`Expected ${result.expected}`)
      }
    }

    if (Array.isArray(parsed)) {
      parsed = await (Promise.all(parsed))
    }

    await handler.init()
    return handler
  }
}


const sources = new FactoryContainer()
const sinks = new FactoryContainer()
const transformers = new FactoryContainer()


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
  const pipe = [] as PipelineComponent<any, any>[]
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


export function register(...aliases: string[]) {
  return function (target: any) {

  }
}


export interface OptionsParser<T> {
  deserialize(unk: unknown): T
}


export abstract class PipelineComponent<O, B> {

  abstract help: string
  abstract options_parser: OptionsParser<O> | null
  options!: O
  abstract body_parser: Parser<B> | null
  body!: B

  async init() {

  }

  /**
   * Handle the chunk stream
   */
  async *handle(upstream: ChunkIterator): ChunkIterator {

  }

}


export abstract class Source<O, B> extends PipelineComponent<O, B> {

  async *handle(upstream: ChunkIterator): ChunkIterator {
    // The source simply forwards everything from upstream
    yield* upstream
    yield* this.emit()
  }

  /**
   * This function must be redefined
   */
  async *emit(): ChunkIterator {

  }

}


export abstract class Sink<O = {}, B = []> extends PipelineComponent<O, B> {

  async *handle(upstream: ChunkIterator): ChunkIterator {
    for await (var chk of upstream) {
      if (chk.type === 'start') {
        yield* this.onCollectionStart(chk)
      } else if (chk.type === 'data') {
        yield* this.onData(chk)
      } else if (chk.type === 'info') {
        yield* this.onInfo(chk)
      } else if (chk.type === 'exec') {
        var method = chk.method
        yield* (this as any)[method](chk.options, chk.body)
      }
    }
  }

  async *onCollectionStart(chunk: Chunk.Start): ChunkIterator {

  }

  async *onData(payload: Chunk.Data): ChunkIterator {

  }

  async *onInfo(chunk: Chunk.Info): ChunkIterator {

  }

}


/**
 * A transformer is a sink, except it is expected of it that it
 * will keep forwarding stuff
 */
export abstract class Transformer<O = {}, B = []> extends Sink<O, B> {

  async *onCollectionStart(c: Chunk.Start): ChunkIterator {
    yield c
  }

  async *onData(c: Chunk.Data): ChunkIterator {
    yield c
  }

  async *onInfo(c: Chunk.Info): ChunkIterator {
    yield c
  }

}