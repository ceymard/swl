// import { Lock } from './streams'
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
    first_payload: any
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

  export function start(name: string, first: any): Start {
    return {type: 'start', name, first_payload: first}
  }

  export function info(source: any, message: string, payload?: any): Info {
    return {type: 'info', source: source.constructor.name, message, payload, level: 10}
  }

}


export type Chunk = Chunk.Start | Chunk.Data | Chunk.Exec | Chunk.Info


export type ChunkIterator = AsyncIterableIterator<Chunk>


/**
 * Conditional pipeline. If the condition is met, then the value travels down
 * another subpipeline
 */
// export function condition(fn: (a: any) => any, components: PipelineComponent<any, any>[]) {

//   const lock = new Lock<any>()
//   const end = Symbol()
//   async function *fake_source(): ChunkIterator {
//     do {
//       var res = await lock.promise
//     } while (res !== end)
//   }

//   components.unshift(fake_source)
//   const pipeline = instantiate_pipeline(components)

//   return async function *condition(upstream: ChunkIterator): ChunkIterator {
//     for await (var chk of upstream) {
//       if (fn(chk)) {
//         // FIXME feed the pipeline
//         lock.resolve(chk)
//         const res = await pipeline.next()
//         if (res.done)
//           return
//         yield res.value
//       } else {
//         yield chk
//       }
//     }
//     // Once upstream is done, we send end down the pipeline
//     // which should trigger the end of the fake_source and thus
//     // trigger the possible sources that were inside this pipe.
//     lock.resolve(end)
//     // We now yield the rest of the pipeline
//     yield* pipeline
//   }
// }


import * as p from 'path'

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


export const sources = new FactoryContainer()
export const sinks = new FactoryContainer()
export const transformers = new FactoryContainer()


/**
 * Connect all the pipeline by sending their generators to the
 * next component as upstream()
 * @param components The pipeline of components to connect.
 */
export function instantiate_pipeline(components: PipelineComponent<any, any>[], initial?: ChunkIterator) {

  async function *start_generator(): ChunkIterator {
    return
  }
  // Connect the handlers between themselves
  // console.log('toto ?', handlers[0].name)
  var comp = components[0].trueHandle(initial ? initial : start_generator())
  for (var c of components.slice(1)) {
    comp = c.trueHandle(comp)
  }

  return comp
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


/**
 * Register a component class
 */
export function register(...mimes: string[]) {
  return function (target: new () => PipelineComponent<any, any>) {
    var proto = Object.getPrototypeOf(target)
    if (proto instanceof Source) {
      sources.add(mimes, target)
    } else if (proto instanceof Transformer) {
      transformers.add(mimes, target)
    } else if (proto instanceof Sink) {
      sinks.add(mimes, target)
    } else {
      sources.add(mimes, target)
      sinks.add(mimes, target)
    }
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

  async end() {

  }

  async error(e: any) {

  }

  async *trueHandle(upstream: ChunkIterator): ChunkIterator {
    try {
      await this.init()
      yield* this.handle(upstream)
    } catch (e) {
      await this.error(e)
    } finally {
      await this.end()
    }
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
    yield chunk
  }

  async *onData(chunk: Chunk.Data): ChunkIterator {
    yield chunk
  }

  async *onInfo(chunk: Chunk.Info): ChunkIterator {
    yield chunk
  }

}


/**
 * A transformer is a sink, except it is expected of it that it
 * will keep forwarding stuff
 */
export abstract class Transformer<O = {}, B = []> extends Sink<O, B> {

}