
import {Duplex, Writable} from 'stream'


export interface CollectionStartPayload {
  name: string
  topology: string[] // ???
}

export interface CollectionEndPayload {
  name: string
}

export interface ErrorPayload {
  error: Error
}

export interface Chunk {
  type: 'chunk' | 'error' | 'finished' | 'start' | 'end'
  payload?: CollectionStartPayload | CollectionEndPayload | ErrorPayload
}


/**
 *
 */
export abstract class Adapter extends Duplex {

  public is_speaking = false
  public is_source = false

  static pipeline(readable: Adapter, ...rest: Adapter[]) {
    // var first = readable
    var iter: Adapter = readable
    iter.is_speaking = true

    for (var r of rest) {
      iter.pipe(r)
      iter = r
    }
  }

  constructor(public uri: string, public options: any = {}, public body: string = '') {
    super({objectMode: true})
  }

  setOptions(options: any) {

  }

  async handle(chunk: Chunk): Promise<Chunk | null | void> {
    var res: any
    const payload = chunk.payload
    if (chunk.type === 'start') {
      res = await this.onCollectionStart(payload)
    } else if (chunk.type === 'end') {
      res = await this.onCollectionEnd(payload)
    } else if (chunk.type === 'chunk') {
      res = await this.onChunk(payload)
    } else if (chunk.type === 'finished') {
      this.is_speaking = true
      res = await this.onUpstreamFinished()
    }

    if (res !== null) {
      return {type: chunk.type, payload: res ? res : payload}
    } else {
      return null
    }
  }

  async onCollectionStart(payload: any): Promise<CollectionStartPayload | null | void> {

  }

  async onCollectionEnd(payload: any) {

  }

  /**
   * Override to do something when the first adapter finished.
   * This may be received only once.
   */
  async onUpstreamFinished(): Promise<null | void> {

  }

  async onChunk(payload: any) {

  }

  async _write(chunk: Chunk, encoding: string, callback: () => any) {
    const modified = await this.handle(chunk)
    if (modified !== null)
      this.push(modified ? modified : chunk)
    callback()
  }

  async writeOther(data: any, writable: Writable) {
    return new Promise((done) => {
      function _write() {
        var ok = writable.write(data)
        if (!ok) {
          writable.once('drain', _write)
        } else {
          done()
        }
      }
      _write()
    })
  }


  _read() {
    // console.log('readin', this.options)
    // console.log('read', arguments)
  }

}

export interface Adapter {
  push(chunk: Chunk, encoding?: string | null): any;
}