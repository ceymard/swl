// import { Lock } from './streams'
import * as s from 'slz'
import { Fragment, ADAPTER_AND_OPTIONS } from './cmdparse'


export type ChunkType =
'start'
| 'data'
| 'exec'
| 'info'


export type PBase = string | number | boolean | null | Date
export type PArray = PBase | PBase[]
export type PObj = PArray | {[name: string]: PArray}
export type Primitive = PObj | {[name: string]: PObj}


export namespace Chunk {
  interface ChunkBase {
    readonly type: ChunkType
  }

  export interface Data extends ChunkBase {
    readonly type: 'data'
    readonly collection: string
    readonly payload: {[name: string]: Primitive}
  }

  export interface Exec extends ChunkBase {
    readonly type: 'exec'
    readonly method: string
    readonly options: any
    readonly body: string
  }

  export function data(collection: string, payload: any): Data {
    return {type: 'data', payload, collection}
  }

}


export type Chunk = Chunk.Data | Chunk.Exec



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

  async send(chunk: Chunk | null) {
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

    if (chunk === null) {
      this.finished = true
    }

    if (this.stack_size >= MAX_STACK_SIZE || chunk === null) {
      var fe = this.fetch_lock
      this.fetch_lock = new Lock
      fe.resolve()
    }

    if (this.stack_size >= MAX_STACK_SIZE) {
      await this.send_lock.promise
    }

  }

  async next() {
    while (!this.start) {
      if (this.finished)
        return null

      await this.fetch_lock.promise
    }
    var start = this.start

    this.stack_size--

    var chunk = start.chunk
    this.start = start.next

    if (!this.start) {
      this.end = null
      var snd = this.send_lock
      this.send_lock = new Lock
      snd.resolve()
    }

    return chunk
  }
}


import * as p from 'path'
import { Lock } from './streams';

export interface Factory {
  new (uri: string, options: any, args: any[]): PipelineComponent
  options_builder: s.Builder<any>
  args_builder: s.ArrayBuilder<any>
}

/**
 * We use this class to store named factories so that the command
 * parser find them.
*/
export class FactoryContainer {
  map = {} as {[name: string]: Factory | undefined}
  all = [] as {component:  Factory, mimes: string[]}[]

  /**
   * Add a factory to the registry
   * @param schema A schema for the options that this factory accepts
   * @param factory The factory function
   * @param mimes Extensions or mime types that this handler accepts
   */
  add(mimes: string[], component: Factory) {
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
   * @param uri The name, extension or mimetype of the handler
   * @param options Provided options to the factory
   * @param args The rest of the command line string
   */
  async get(uri: string, options: any, args: any[]): Promise<PipelineComponent | null> {
    var factory = this.map[uri]

    if (!factory) {
      // Try to see if we have a URI (like protocol://some_uri), in which
      // case the protocol name will be used to find the correct factory
      const re_uri = /^([\w]+):\/\//
      const match = re_uri.exec(uri)
      if (match) {
        factory = this.map[match[1]]
        // in that case, we elide the protocol from the string
        // rest = `${name.replace(match[0], '')} ${rest}`
      }
    }

    if (!factory) {
      // Try to parse a regular file path name and from its extension
      // get a factory.
      const a = p.parse(uri)
      factory = this.map[a.ext]
    }

    if (!factory) return null

    // This is probably not what we want to do !!!
    var opts = factory.options_builder.from(options)
    var arg = factory.args_builder.from(args)

    if (opts.isError())
      throw new Error(`in ${factory.name} options: ${opts.errors}`)

    if (arg.isError())
      throw new Error(`in ${factory.name} args: ${arg.errors}`)

    var handler = new factory(uri, opts.value, arg.value)

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
  return function (target: Factory) {
    var proto = Object.getPrototypeOf(target)
    proto = new proto
    if (proto === SourceComponent || proto instanceof SourceComponent) {
      sources.add(mimes, target)
    } else if (proto === TransformerComponent || proto instanceof TransformerComponent) {
      transformers.add(mimes, target)
    } else if (proto === SinkComponent || proto instanceof SinkComponent) {
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
export function instantiate_pipeline(components: PipelineComponent[], initial?: ChunkStream) {

  if (!initial) {
    initial = new ChunkStream()
    initial.send(null)
  }

  // Connect the handlers between themselves
  var stream = initial
  for (var c of components) {
    c.runComponent(stream).catch(e => console.error(e.stack))
    stream = c.stream
  }

  return stream
}


export async function build_pipeline(fragments: Fragment[]) {
  const pipe = [] as PipelineComponent[]
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



export const MAX_STACK_SIZE = 8192


export class PipelineComponent {

  help: string = 'No help provided by implementor'
  static options_builder: s.Builder<any>
  static args_builder: s.ArrayBuilder<any>

  upstream!: ChunkStream
  stream = new ChunkStream()

  /**
   * Send chunks down the pipeline
   * @param chk The Chunk to send. If null, it means this
   *  component is finished sending stuff.
   * @returns null if the sending went well or a Lock if its stack_size
   *  reached size limit.
   */
  async send(chk: Chunk | null) {
    return await this.stream.send(chk)
  }

  async init() {

  }

  async end() {

  }

  async final() {

  }

  info(message: string, payload?: any) {
    var err = process.stderr
    err.write(info(`${this.constructor.name}: `) + message)
    if (payload !== undefined) {
      print_value(err, payload)
    }
    err.write('\n')
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
      try {
        await this.error(e)
      } catch (e2) {
        console.error('error in error()', e2.stack)
      }
      throw e
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
    while (next = await this.upstream.next()) {
      await this.send(next)
    }
  }
}


export class SourceComponent extends PipelineComponent {

  async process() {
    // The source simply forwards everything from upstream
    await this.forward(this.upstream)
    await this.emit()
    this.send(null)
  }

  /**
   * This function must be redefined
   */
  async emit(): Promise<void> { }

}


export class SinkComponent extends PipelineComponent {

  async process() {
    var current_collection: string | null = null

    var chk: Chunk | Promise<any> | null
    while (chk = await this.upstream.next()) {
      if (chk.type === 'data') {
        if (current_collection !== chk.collection) {
          if (current_collection !== null)
            await this.onCollectionEnd(current_collection)
          current_collection = chk.collection
          await this.onCollectionStart(chk)
        }
        await this.onData(chk)
      } else if (chk.type === 'exec') {
        var method = chk.method
        await (this as any)[method](chk.options, chk.body)
      }
    }

    if (current_collection)
      await this.onCollectionEnd(current_collection)

    // We're done, so we're sending null !
    await this.send(null)
  }

  async onCollectionStart(chunk: Chunk.Data) {

  }

  async onCollectionEnd(name: string) {

  }

  async onData(chunk: Chunk.Data) {
    await this.send(chunk)
  }

}


/**
 * A transformer is a sink, except it is expected of it that it
 * will keep forwarding stuff
 */
export abstract class TransformerComponent extends SinkComponent {

}


export function Source<T, U>(options: s.Builder<T>, args: s.ArrayBuilder<U>) {
  return class Src extends SourceComponent {

    static options_builder = options
    static args_builder = args

    constructor(public uri: string, public options: T, public args: U[]) { super() }

    async emit() {

    }
  }
}

export function Sink<T, U>(options: s.Builder<T>, builder: s.ArrayBuilder<U>) {

  return class Snk extends SinkComponent {
    static builder = builder
    constructor(public uri: string, public options: T, public args: U[]) { super() }
    builder = builder
  }

}

export function Transformer<T, U>(options: s.Builder<T>, args: s.ArrayBuilder<U>) {
  return class Transformer extends TransformerComponent {
    static options_builder = options
    static args_builder = args
    constructor(public uri: string, public options: T, public args: U[]) { super() }
  }
}


import { info, print_value } from './sinks/debug'

