import {Adapter} from './adapter'
import {basename} from 'path'
import {createWriteStream, WriteStream} from 'fs'
import {promisify} from 'util'

export interface JsonAdapterOptions {
  name: string
}

export class JsonAdapter extends Adapter {

  done = false
  is_source = !!this.body
  file: WriteStream | null = !this.is_source ? createWriteStream(this.uri, {flags: 'w', encoding: 'utf-8'}) : null

  async onUpstreamFinished(): Promise<null | void> {
    this._read()
    return null
  }

  async onChunk(chunk: any) {
    if (this.file) {
      const wr = this.file.write.bind(this.file)
      await promisify(wr)(JSON.stringify(chunk))
    }
  }

  _read() {

    if (this.is_speaking && this.is_source && !this.done) {
      const collection = basename(this.uri)
      this.done = true
      const bod = JSON.parse(`[${this.body}]`)
      // console.log(bod)
      this.push({type: 'start', payload: {name: collection}})
      for (var b of bod) {
        this.push({type: 'chunk', payload: b})
      }
      this.push({type: 'end', payload: {name: collection}})
      this.push({type: 'finished'})
      this.end()
    }

  }

}
