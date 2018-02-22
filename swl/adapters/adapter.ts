
(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

import {EventEmitter} from 'events'
import { PassThrough } from 'stream';

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

// export interface PipelineEvent {
//   type: EventType
//   cleared_on?: {[pipe_id: string]: true}
//   emitter: string
//   payload: any
// }


export type WriteStreamCreator = (colname: string) => Promise<NodeJS.WritableStream> | NodeJS.WritableStream


async function resume_once(em: EventEmitter, event: string) {
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


export class StreamSource extends Source {

  _codec: NodeJS.ReadWriteStream | null = null

  constructor(options: any, public source: NodeJS.ReadableStream) {
    super(options)
  }

  /**
   * Set a codec on this stream source
   * @param codec The codec to set this source to.
   */
  async codec(): Promise<NodeJS.ReadWriteStream> {
    return new PassThrough()
  }

  async *handleChunk(chk: any): AsyncIterableIterator<PipelineEvent> {
    yield this.data(chk)
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    var res: any
    var ended = false
    const src = this.source.pipe(await this.codec())
    yield this.start((this.source as NodeJS.ReadStream).path)
    src.on("end", () => { ended = true })
    do {
      // console.log('??')

      res = src.read()
      if (res != null)
        yield* this.handleChunk(res)
      if (ended)
        return
      await resume_once(src, 'readable')
    } while (true)
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
