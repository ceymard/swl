
(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

import {EventEmitter} from 'events'

export type EventType =
    'start'
  | 'data'
  | 'exec'


export interface PipelineEventBase {
  type: EventType
  emitter: string
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


export type WriteStreamCreator = (colname: string) => Promise<NodeJS.WritableStream> | NodeJS.WritableStream


export async function resume_once(em: EventEmitter, event: string) {
  var acc: Function
  const prom = new Promise((accept) => {
    acc = accept
  })

  em.once(event, () => acc())

  return prom
}



export class PipelineComponent {

  public prev!: PipelineComponent
  public sub_pipe_id: string = ''
  started = false

  protected handlers = {}

  constructor(public options: any) {

  }

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
export class Source extends PipelineComponent {

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
      if (this.ended)
        return null
      await resume_once(this.source, 'readable')
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
      this.source.on('end', () => this.ended = true)
      yield* this.handleSource()
    }
  }

}


export abstract class StreamSink extends PipelineComponent {

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


export async function pipeline(first: PipelineComponent, ...rest: PipelineComponent[]) {
  var iter = first
  for (var r of rest) {
    r.prev = iter
    iter = r
  }

  for await (var pkt of iter.events()) {
    pkt // Do nothing.
  }
}
