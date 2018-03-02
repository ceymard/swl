
(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

import {EventEmitter} from 'events'
import { register_sink } from 'swl/register';

export type EventType =
    'start'
  | 'data'
  | 'exec'


export interface PipelineEventBase {
  type: EventType
  emitter?: string
  // cleared_on?: {[pipe_id: string]: true}
}

export interface StartEvent extends PipelineEventBase {
  type: 'start'
  name: string
}

export interface DataEvent extends PipelineEventBase {
  type: 'data'
  payload: any
}

export interface ExecEvent extends PipelineEventBase {
  type: 'exec'
  method: string
  options: any
  body: string
}

export type PipelineEvent = StartEvent | DataEvent | ExecEvent


export namespace PipelineEvent {
  export function start(name: string): StartEvent {
    return {type: 'start', name}
  }
}


export type Flow = AsyncIterableIterator<PipelineEvent>

export type Node = (upstream: Flow) => Flow

export type WriteStreamCreator = (colname: string) => Promise<NodeJS.WritableStream> | NodeJS.WritableStream


export async function resume_once(em: EventEmitter, ...events: string[]) {
  var acc: Function
  const prom = new Promise((accept) => {
    acc = accept
  })

  function handle() {
    for (var e2 of events)
      em.removeListener(e2, handle)
    acc()
  }

  for (var e of events) {
    em.on(e, handle)
  }

  return prom
}



export class Sink {

  public prev!: Sink
  public sub_pipe_id: string = ''
  started = false

  protected handlers = {}

  constructor(public options: any) { }

  start(name: string): StartEvent {
    return {type: 'start', name, emitter: this.constructor.name}
  }

  data(payload: any): DataEvent {
    return {type: 'data', payload, emitter: this.constructor.name}
  }

  exec(method: string, options: any, body: string): ExecEvent {
    return {
      type: 'exec',
      method,
      options,
      body,
      emitter: this.constructor.name
    }
  }

  upstream() {
    return this.prev ? this.prev.events() : []
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {
    yield* this.upstream()
  }

  async *events() {
    yield* this.process()
  }

}


/**
 * A source only
 */
export class Source extends Sink {

  /**
   * Method that the sources will have to implement.
   * emit() is called when this source should be starting to emit stuff.
   */
  async *emit(): AsyncIterableIterator<PipelineEvent> {
    // There will probably be a bunch of write() here.
  }

  async *events() {
    // Yield whatever came before
    yield* this.process()
    // Now that upstream is done, we can start emitting.
    yield* this.emit()
  }

}

export class Transformer extends Sink {
  async *process(): AsyncIterableIterator<PipelineEvent> {
    for await (var ev of this.upstream()) {
      if (ev.type === 'data') {
        yield this.data(this.transform(ev.payload))
      } else yield ev
    }
  }

  transform(payload: any): any {
    return payload
  }
}


export type Sources = AsyncIterableIterator<{
  collection: string
  source: NodeJS.ReadableStream
}>


export class StreamSource extends Source {

  ended = false
  collection: string = ''
  source!: NodeJS.ReadableStream

  constructor(options: any, public sources: Sources) {
    super(options)
  }

  /**
   *
   * @param source
   */
  async nextSource(source: NodeJS.ReadableStream): Promise<NodeJS.ReadableStream> {
    return source
  }

  async read(): Promise<any | null> {
    do {
      var res = this.source.read()
      if (res !== null)
        return res
      if (this.ended) {
        // console.log('stream ended')
        return null
      }
      await resume_once(this.source, 'readable', 'end')
    } while (true)
  }

  async *handleSource(): AsyncIterableIterator<PipelineEvent> {
    var res
    yield this.start(this.collection)
    while ((res = await this.read()) !== null) {
      yield this.data(res)
    }
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    // const src = this.source.pipe(await this.codec())
    for await (var src of this.sources) {
      this.collection = src.collection
      this.source = await this.nextSource(src.source)
      this.source.on('end', () => {
        // console.log('ended ?')
        this.ended = true
      })
      yield* this.handleSource()
    }
  }

}


export abstract class StreamSink extends Sink {

  writable!: NodeJS.ReadWriteStream
  stream!: NodeJS.WritableStream

  constructor(options: any, public creator: WriteStreamCreator) {
    super(options)
  }

  abstract async codec(): Promise<NodeJS.ReadWriteStream>

  async *process(): AsyncIterableIterator<PipelineEvent> {
    var stream: NodeJS.WritableStream | undefined
    var writable: NodeJS.ReadWriteStream | undefined

    function close() {
      if (writable) writable.end()
      if (stream) stream.end()
    }

    for await (var ev of this.upstream()) {
      if (ev.type === 'start') {
        close()
        stream = await this.creator(ev.name)
        writable = await this.codec()
        writable.pipe(stream)
      } else if (ev.type === 'data') {
        if (!writable!.write(ev.payload))
          await resume_once(writable!, 'drain')
      }
    }

    close()
  }

}


export function simple_transformer(fn: (opts: any, a: any) => any, ...mimes: string[]) {

  class SimpleTransformer extends Sink {
    async *process(): AsyncIterableIterator<PipelineEvent> {
      const opts = this.options
      for await (var ev of this.upstream()) {
        if (ev.type === 'data') {
          yield this.data(fn(opts, ev.payload))
        } else yield ev
      }
    }
  }

  register_sink(async (opts: any) => {
    return new SimpleTransformer(opts)
  }, ...mimes)
}


export async function pipeline(first: Sink, ...rest: Sink[]) {
  var iter = first
  for (var r of rest) {
    r.prev = iter
    iter = r
  }

  for await (var pkt of iter.events()) {
    pkt // Do nothing.
  }
}
