
import {Duplex, Writable} from 'stream'
import * as yup from 'yup'

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


export interface AdapterCreator {
  new (uri: string, options: any, body: string): Adapter<any>
}

export const registry: {
  [name: string]: AdapterCreator
} = {}

export const mime_registry: {
  [name: string]: AdapterCreator
} = {}

/**
 *
 */
export abstract class Adapter<O extends Object> extends Duplex {

  public is_speaking = false
  public is_source = false

  schema = yup.object()

  static register(...uri: string[]) {
    for (var u of uri)
      registry[u] = this as any
    return this
  }

  static registerMime(...mimes: string[]) {
    for (var u of mimes)
      mime_registry[u] = this as any
    return this
  }

  static pipeline(...rest: Adapter<any>[]) {
    // var first = readable
    var iter: Adapter<any> = rest[0]
    rest = rest.slice(1)
    iter.is_speaking = true

    for (var r of rest) {
      iter.pipe(r)
      iter = r
    }
  }

  constructor(public uri: string, public options: O = {} as any, public body: string = '') {
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

  async onChunk(payload: any): Promise<null | any> {

  }

  async _write(chunk: Chunk, encoding: string, callback: () => any) {
    const modified = await this.handle(chunk)
    if (modified !== null)
      this.push(modified ? modified : chunk)
    callback()
  }

  async writeOther(writable: Writable, data: any) {
    return new Promise((done) => {
      if (!writable.write(data))
        writable.once('drain', done)
      else done()
    })
  }

  /**
   * An even more private version of _read !
  */
  __read() {

  }

  _read() {
    if (this.is_speaking && this.is_source) {

      this.__read()
    }
    // console.log('readin', this.options)
    // console.log('read', arguments)
  }

}

export interface Adapter<O extends Object> {
  push(chunk: Chunk, encoding?: string | null): any;
}

export class Source {

}

export class Transformer {

}

/**
 * Are sinks that different from transformers ?
 */
export class Sink {

}