
import {EventEmitter} from 'events'

export type EventType =
    'start'
  | 'data'
  | 'end'
  | 'error'
  | 'exec'


export interface PipelineEvent {
  type: EventType
  cleared_on?: {[pipe_id: string]: true}
  emitter: string
  payload: any
}


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

  async onstart(payload: any): Promise<any> {

  }

  async ondata(payload: any): Promise<any> {

  }

  async onstop(payload: any): Promise<any> {

  }

  async onend(payload: any): Promise<any> {

  }

  async onexec(payload: any): Promise<any> {

  }

  async onerror(payload: any): Promise<any> {

  }

  event(type: EventType, payload: any): PipelineEvent {
    return {type, payload, emitter: this.constructor.name}
  }

  async process(event: PipelineEvent) {
    var res: any
    var {type, payload} = event

    if (type === 'start') {
      if (this.started) {
        await this.onstop(null)
      }
      this.started = true
      res = await this.onstart(payload)
    } else if (type === 'data') {
      res = await this.ondata(payload)
    } else if (type === 'end') {
      if (this.started) {
        await this.onstop(null)
        this.started = false
      }
      res = await this.onend(payload)
    } else if (type === 'exec') {
      res = await this.onexec(payload)
    } else if (type === 'error') {
      res = await this.onerror(payload)
    }

    return res === null ? null :
      res ? this.event(type, res) : event
  }

  async *processUpstream(): AsyncIterableIterator<PipelineEvent> {
    if (!this.prev) return

    for await (var ev of this.prev.events()) {
      var res = await this.process(ev)
      if (res !== null)
        yield res
      // console.log(ev)
    }

  }

  async *events() {
    yield* this.processUpstream()
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
    yield* this.processUpstream()
    // Now that upstream is done, we can start emitting.
    yield* this.emit()
  }

}


export class StreamSource extends Source {

  ended = false
  _codec: NodeJS.ReadWriteStream | null = null

  constructor(public source: NodeJS.ReadableStream) {
    super()
    source.on('end', () => {
      this.ended = true
    })
  }

  /**
   * Set a codec on this stream source
   * @param codec The codec to set this source to.
   */
  codec(codec: NodeJS.ReadWriteStream) {
    this.source = this.source.pipe(codec)
  }

  async readSource() {
    var res: any
    while ( (res = this.source.read()) ) {
      if (res != null)
        return res
      if (this.ended)
        return null
      await resume_once(this.source, 'readable')
    }
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {

  }

}


export abstract class StreamSink extends PipelineComponent {

  writable!: NodeJS.ReadWriteStream
  stream!: NodeJS.WritableStream

  constructor(public options: any, public creator: WriteStreamCreator) {
    super()
  }

  abstract async codec(): Promise<NodeJS.ReadWriteStream>

  async onstart(name: string) {
    // this.writable.unpipe()

    this.stream = await this.creator(name)
    this.writable = await this.codec()
    this.writable.pipe(this.stream)
  }

  async onstop() {
    this.writable!.unpipe()
    this.stream.end()
  }

  async ondata(chk: any) {
    await this.output(chk)
  }

  async output(chk: any) {
    if (!this.writable!.write(chk))
      await resume_once(this.writable!, 'drain')
  }

}


export async function pipeline(first: PipelineComponent, ...rest: PipelineComponent[]) {
  var iter = first
  for (var r of rest) {
    r.prev = iter
    iter = r
  }

  for await (var pkt of iter.events()) {
    // Do nothing.
  }
}
