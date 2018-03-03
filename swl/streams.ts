import {createWriteStream, createReadStream} from 'fs'
import {Sources} from './adapters'

export async function make_write_creator(uri: string, options: any) {
  // Check for protocol !!
  var glob = uri.indexOf('*') > -1
  var stream: NodeJS.WritableStream
  return function (name: string) {
    if (!glob && stream) return stream
    return createWriteStream(uri.replace('*', name), options)
  }
}

import * as path from 'path'

export async function *make_read_creator(uri: string, options: any): Sources {
  // Check for protocol !!
  yield {
    collection: path.basename(uri).replace(/\.[^\.]+$/, ''),
    source: createReadStream(uri, options) as NodeJS.ReadableStream
  }
}


export class Lock<T = void> {
  private _prom: Promise<T> | null = null
  private _resolve: null | ((val: T) => void) = null
  private _reject: null | ((err: any) => void) = null

  get promise(): Promise<T> {
    if (!this._prom) {
      this._prom = new Promise<T>((resolve, reject) => {
        this._resolve = resolve
        this._reject = reject
      })
    }
    return this._prom
  }

  private reset() {
    this._prom = null
    this._resolve = null
    this._reject = null
  }

  resolve(this: Lock<void>): void
  resolve(value: T): void
  resolve(value?: T) {
    if (!this._resolve) {
      this.promise
    }
    const res = this._resolve!
    this.reset()
    res(value!)
  }

  reject(err: any) {
    if (!this._reject) { this.promise }
    const rej = this._reject!
    this.reset()
    rej(err)
  }
}



export class StreamWrapper<T extends NodeJS.ReadableStream | NodeJS.WritableStream> {

  _ended: boolean = false
  should_drain = false
  ended = new Lock()
  readable = new Lock()
  drained = new Lock()

  constructor(public stream: T, public collection?: string) {
    stream.on('readable', e => this.readable.resolve())
    stream.on('end', e => {
      this._ended = true
      this.ended.resolve()
      this.readable.resolve()
    })
    stream.on('drain', e => {
      this.drained.resolve()
    })
    stream.on('error', e => {
      this.ended.reject(e)
      this.readable.reject(e)
      this.drained.reject(e)
    })
  }

  /**
   *
   */
  async read<U extends NodeJS.ReadableStream>(this: StreamWrapper<U>) {
    do {
      var res = this.stream.read()

      if (res !== null)
        return res

      if (this._ended) {
        // console.log('stream ended')
        return null
      }
      await this.readable.promise
    } while (true)

  }

  /**
   * Write data to the streama
   */
  async write<U extends NodeJS.WritableStream>(this: StreamWrapper<U>, data: any) {
    if (this.should_drain) {
      await this.drained.promise
      this.should_drain = false
    }

    if (!this.stream.write(data))
      this.should_drain = true
  }

  close<U extends NodeJS.WritableStream>(this: StreamWrapper<U>) {
    this.stream.end()
  }

}