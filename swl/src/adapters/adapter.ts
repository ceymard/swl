
import {Chunk} from '../types'
import {Readable, Duplex, Writable} from 'stream'


/**
 *
 */
export abstract class Adapter extends Duplex {

  static pipeline(readable: Readable, ...rest: Duplex[]) {
    var iter = readable
    for (var r of rest) {
      iter.pipe(r)
      iter = r
    }

    // Last iterable gets a default writable that does nothing.
    iter.pipe(new Writable({objectMode: true, write() {
      // does nothing !
    }}))
  }

  constructor(public options: any = {}, public body: any = {}) {
    super({objectMode: true})
  }

  setOptions(options: any) {

  }

  abstract async handle(chunk: Chunk): Promise<Chunk | void>;

  async _write(chunk: Chunk, encoding: string, callback: () => any) {
    const modified = await this.handle(chunk)
    callback()
    this.push(modified ? modified : chunk)
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
