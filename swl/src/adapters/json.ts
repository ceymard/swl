import {Adapter, CollectionStartPayload} from './adapter'
import {basename} from 'path'
import {createWriteStream, WriteStream} from 'fs'


export interface JsonAdapterOptions {
  beautify?: boolean
  object?: boolean
}

export class JsonAdapter extends Adapter<JsonAdapterOptions> {

  done = false
  first = true
  is_source = !!this.body
  file: WriteStream | null = !this.is_source ? createWriteStream(this.uri, {flags: 'w', encoding: 'utf-8'}) : null

  beautify = !!this.options.beautify
  as_object = !!this.options.object

  async onUpstreamFinished(): Promise<null | void> {
    if (this.is_source) {
      this._read()
      return null
    } else {
      this.output(']\n')
    }
  }

  async onCollectionStart(payload: CollectionStartPayload) {
    if (!this.is_source && this.as_object) {
      this.output(`"${payload.name}": [`)
    }
  }

  async onCollectionEnd() {
    if (!this.is_source && this.as_object) {
      this.output(']')
    }
  }

  async onChunk(chunk: any) {
    if (!this.is_source) {
      if (this.first) {
        this.first = false
        this.as_object ? this.output('{') : this.output('[')
      } else {
        this.output(this.beautify ? ',\n' : ',')
      }
      this.output(JSON.stringify(chunk, null, this.beautify ? '  ' : ''))
      return null
    }
  }

  output(data: any) {
    if (!this.file) return
    this.writeOther(this.file, data)
  }

  __read() {
    if (!this.done) {
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

JsonAdapter
  .register('json')
  .registerMime('application/json')
