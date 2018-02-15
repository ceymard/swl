
import {Duplex, Writable} from 'stream'


export interface Chunk {
  type: 'chunk' | 'error' | 'end' | 'collection-start' | 'collection-end'
  payload?: any
}


/**
 *
 */
export abstract class Adapter extends Duplex {

  public is_first = false

  static pipeline(readable: Adapter, ...rest: Adapter[]) {
    var first = readable
    var iter = readable
    iter.is_first = true

    for (var r of rest) {
      iter.pipe(r)
      iter = r
    }

    // first.read()

    // Last iterable gets a default writable that does nothing.
    // iter.pipe(new Writable({objectMode: true, write() {
    //   // does nothing !
    // }}))
  }

  constructor(public options: any = {}, public body: string = '') {
    super({objectMode: true})
  }

  setOptions(options: any) {

  }

  abstract async handle(chunk: Chunk): Promise<Chunk | null | void>;

  async onCollectionStart(payload: any) {

  }

  async onCollectionEnd(payload: any) {

  }

  /**
   * Override to do something when the first adapter finished.
   * This may be received only once.
   */
  async onUpstreamEnd() {

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