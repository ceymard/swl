// import { Lock } from './streams'
import { Parser } from 'parsimmon'
import { Fragment, ADAPTER_AND_OPTIONS } from './cmdparse'

export type ChunkType =
'start'
| 'data'
| 'exec'
| 'info'

export namespace Chunk {
  interface ChunkBase {
    readonly type: ChunkType
  }

  export interface Data extends ChunkBase {
    readonly type: 'data'
    readonly collection: string
    readonly payload: any
  }

  export interface Exec extends ChunkBase {
    readonly type: 'exec'
    readonly method: string
    readonly options: any
    readonly body: string
  }

  export interface Info extends ChunkBase {
    readonly type: 'info'
    readonly level: number
    readonly source: string
    readonly message: string
    readonly payload: any
  }

  export function data(collection: string, payload: any): Data {
    return {type: 'data', payload, collection}
  }

  export function info(source: any, message: string, payload?: any): Info {
    return {type: 'info', source: source.constructor.name, message, payload, level: 10}
  }

}


export type Chunk = Chunk.Data | Chunk.Exec | Chunk.Info



interface LLChunk {
  chunk: Chunk | null
  next: LLChunk | null
}


export class ChunkStream {
  start: LLChunk | null = null
  end: LLChunk | null = null
  stack_size = 0

  finished = false
  send_lock = new Lock
  fetch_lock = new Lock

  send(chunk: Chunk | null) {
    if (this.finished) throw new Error(`Finished stream can't be written unto`)

    var ll = {chunk, next: null}

    if (this.stack_size === 0) {
      this.start = ll
      this.end = ll
    } else {
      this.end!.next = ll
      this.end = ll
    }

    this.stack_size++

    if (chunk === null) this.finished = true

    if (this.stack_size >= MAX_STACK_SIZE) {
      return this.send_lock.promise
    }
  }

  next() {
    var start = this.start
    if (!start) {
      if (this.finished)
        return null
      return this.fetch_lock.promise
    }

    this.stack_size--

    var chunk = start.chunk
    this.start = start.next

    if (!this.start) this.end = null

    return chunk
  }
}


import * as p from 'path'
import { Lock } from './streams';

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
        handler.body = parsed
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
 * Register a component class
 */
export function register(...mimes: string[]) {
  return function (target: new () => PipelineComponent<any, any>) {
    var proto = Object.getPrototypeOf(target)
    if (proto === Source || proto instanceof Source) {
      sources.add(mimes, target)
    } else if (proto === Transformer || proto instanceof Transformer) {
      transformers.add(mimes, target)
    } else if (proto === Sink || proto instanceof Sink) {
      sinks.add(mimes, target)
    } else {
      sources.add(mimes, target)
      sinks.add(mimes, target)
    }
  }
}


/**
 * Connect all the pipeline by sending their generators to the
 * next component as upstream()
 * @param components The pipeline of components to connect.
 */
export function instantiate_pipeline(components: PipelineComponent<any, any>[], initial?: ChunkStream) {

  if (!initial) {
    initial = new ChunkStream()
    initial.send(null)
  }

  // Connect the handlers between themselves
  var stream = initial
  for (var c of components) {
    c.runComponent(stream)
    stream = c.stream
  }

  return stream
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


export interface OptionsParser<T> {
  deserialize(unk: unknown): T
}


export const MAX_STACK_SIZE = 1024


export abstract class PipelineComponent<O, B> {

  abstract help: string
  abstract options_parser: OptionsParser<O> | null
  options!: O
  abstract body_parser: Parser<B> | null
  body!: B

  upstream!: ChunkStream
  stream = new ChunkStream()

  send_lock: Lock | null = null
  fetch_lock: Lock | null = null
  protected stack_size: number = 0

  /**
   * Send chunks down the pipeline
   * @param chk The Chunk to send. If null, it means this
   *  component is finished sending stuff.
   * @returns null if the sending went well or a Lock if its stack_size
   *  reached size limit.
   */
  send(chk: Chunk | null) {
    return this.stream.send(chk)
  }

  async init() {

  }

  async end() {

  }

  async final() {

  }

  async error(e: any) {

  }

  async runComponent(upstream: ChunkStream) {
    this.upstream = upstream
    try {
      await this.init()
      await this.process()
      await this.end()
    } catch (e) {
      await this.error(e)
    } finally {
      await this.final()
    }
  }

  /**
   * Handle the chunk stream
   */
  async process() {

  }

  async forward(stream: ChunkStream) {
    var next: Chunk | Promise<any> | null
    while (next = this.upstream.next()) {

      if (next instanceof Promise) {
        await next
        continue
      }

      this.send(next)
    }
  }
}


export abstract class Source<O, B> extends PipelineComponent<O, B> {

  async process() {
    // The source simply forwards everything from upstream
    await this.forward(this.upstream)
    await this.emit()
    this.send(null)
  }

  /**
   * This function must be redefined
   */
  abstract async emit(): Promise<void>

}


export abstract class Sink<O = {}, B = []> extends PipelineComponent<O, B> {

  async process() {
    var current_collection: string | null = null

    var chk: Chunk | Promise<any> | null
    while (chk = this.upstream.next()) {

      if (chk instanceof Promise) {
        await chk
        continue
      }

      if (chk.type === 'data') {
        if (current_collection !== chk.collection) {
          if (current_collection !== null)
            await this.onCollectionEnd(current_collection)
          current_collection = chk.collection
          this.onCollectionStart(chk)
        }
        await this.onData(chk)
      } else if (chk.type === 'info') {
        await this.onInfo(chk)
      } else if (chk.type === 'exec') {
        var method = chk.method
        await (this as any)[method](chk.options, chk.body)
      }
    }

    if (current_collection)
      await this.onCollectionEnd(current_collection)

    // We're done, so we're sending null !
    await this.send(null)
    await this.end()
  }

  async onCollectionStart(chunk: Chunk.Data) {

  }

  async onCollectionEnd(name: string) {

  }

  async onData(chunk: Chunk.Data) {
    this.send(chunk)
  }

  async onInfo(chunk: Chunk.Info) {
    this.send(chunk)
  }

}


/**
 * A transformer is a sink, except it is expected of it that it
 * will keep forwarding stuff
 */
export abstract class Transformer<O = {}, B = []> extends Sink<O, B> {

}