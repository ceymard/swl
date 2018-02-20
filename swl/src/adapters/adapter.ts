
import {EventEmitter} from 'events'

export type EventType =
    'start'
  | 'data'
  | 'end'
  | 'error'
  | 'exec'


export interface Chunk {
  payload: PipelineEvent
  next: Chunk | null
}

export interface PipelineEvent {
  type: EventType
  cleared_on?: {[pipe_id: string]: true}
  payload: any
}

export class Lock {
  waiting = false
  prom: Function | null = null

  release() {
    this.waiting = false
    if (this.prom) {
      this.prom()
      this.prom = null
    }
  }

  wait(): Promise<void> {
    this.waiting = true
    return new Promise((accept) => {
      this.prom = accept
    })
  }
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


/**
 * We don't allow more than this many objects into the stream.
 */
export var MAX_STACK_LENGTH = 1024


export class ChunkStack {
  first: Chunk | null = null
  last: Chunk | null = null
  count: number = 0
  private wlock = new Lock()
  private rlock = new Lock()

  isFull() {
    return this.count >= MAX_STACK_LENGTH
  }

  async write(type: EventType, payload: any) {
    const ch = {
      payload: {type, payload}, next: null
    }

    if (this.count >= MAX_STACK_LENGTH) {
      await this.wlock.wait()
    }

    if (this.count === 0) {
      this.first = ch
      this.last = ch
    } else {
      this.last!.next = ch
      this.last = ch
    }
    this.count++
    if (this.rlock.waiting) this.rlock.release()
  }

  async read(): Promise<PipelineEvent | null> {
    if (this.count === 0) {
      await this.rlock.wait()
    }
    var ch = this.first!

    this.first = ch.next
    if (ch.next === null)
      this.last = null

    this.count--
    if (this.wlock.waiting) this.wlock.release()
    return ch.payload
  }

}



export class PipelineComponent {

  public prev!: PipelineComponent
  public sub_pipe_id: string = ''
  started = false

  protected out = new ChunkStack()
  protected handlers = {}

  constructor() {

  }

  async send(type: EventType, payload?: any) {
    await this.out.write(type, payload)
  }

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

  async process(type: EventType, payload?: any) {
    var res: any

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

    if (res !== null) {
      this.send(type, res ? res : payload)
    }
  }

  async readFromUpstream() {
    var ch: PipelineEvent | null
    var prev_out = this.prev.out

    while ( (ch = await prev_out.read()) ) {
      var {type, payload} = ch!
      try {
        await this.process(type, payload)
      } catch (e) {
        // FIXME ?
        // console.log(e.stack)
      }
      // Handle the payloads here, redispatch to the correct methods (?)
    }
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
  async emit() {
    // There will probably be a bunch of write() here.
  }

  /**
   * Handle an upstream end as the fact that we'll start talking now.
   */
  async onend() {
    // We will emit shortly.
    setTimeout(() => this.emit(), 0)
    return null
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

  async emit() {

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


export function pipeline(first: PipelineComponent, ...rest: PipelineComponent[]) {
  var iter = first
  for (var r of rest) {
    r.prev = iter
    iter = r
    iter.readFromUpstream()
  }

  // Get the first component to speak (it should be a source !)
  first.process('end')
}
