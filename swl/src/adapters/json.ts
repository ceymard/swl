import {Adapter, Chunk} from './adapter'

export interface JsonAdapterOptions {
  name: string
}

export class JsonAdapter extends Adapter {

  done = false

  async handle(chunk: Chunk) {
    if (chunk.type === 'end') {
      // console.log('ended')
      this.is_first = true
      this._read()
      return null
    }

    // console.log('body !', chunk)
    // Json adapter does nothing with a chunk, it just passes it along.
  }

  _read() {

    if (this.is_first && !this.done && this.body) {
      this.done = true
      const bod = JSON.parse(`[${this.body}]`)
      // console.log(bod)
      this.push({type: 'collection-start', payload: this.options.name})
      for (var b of bod) {
        this.push({type: 'chunk', payload: b})
      }
      this.push({type: 'collection-end', payload: this.options.name})
      this.push({type: 'end'})
    }

  }

}
