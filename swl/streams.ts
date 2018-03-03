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




export class StreamWrapper<T extends NodeJS.ReadableStream | NodeJS.WritableStream> {

  _error: any = undefined
  _ended: boolean = false

  constructor(public stream: T, public collection?: string) { }

  async resume_once(...events: string[]) {
    var acc: Function
    const prom = new Promise((accept) => {
      acc = accept
    })

    const em = this.stream
    function handle() {
      for (var e2 of events)
        em.removeListener(e2, handle)
      acc()
    }

    for (var e of events) {
      em.on(e, handle)
    }
    em.on('end', () => {
      this._ended = true
    })

    return prom
  }

  /**
   *
   */
  async read<U extends NodeJS.ReadableStream>(this: StreamWrapper<U>) {
    do {
      var res = this.stream.read()
      if (res !== null)
        return res
      if (this._error) {
        throw this._error
      }
      if (this._ended) {
        // console.log('stream ended')
        return null
      }
      await this.resume_once('readable', 'end', 'error')
    } while (true)

  }

  /**
   * Write data to the streama
   */
  async write<U extends NodeJS.WritableStream>(this: StreamWrapper<U>, data: any) {
    await this.resume_once('drain')
  }

}