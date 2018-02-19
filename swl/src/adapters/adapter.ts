
export type EventType =
    'start'
  | 'data'
  | 'stop'
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
      res = await this.onstart(payload)
    } else if (type === 'data') {
      res = await this.ondata(payload)
    } else if (type === 'stop') {
      res = await this.onstop(payload)
    } else if (type === 'end') {
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
