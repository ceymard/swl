import {Adapter} from './adapter'

export interface JsonAdapterOptions {
  name: string
}

export class JsonAdapter extends Adapter {

  done = false

  async onUpstreamFinished(): Promise<null | void> {
    this._read()
    return null
  }

  _read() {

    if (this.is_source && !this.done && this.body) {
      this.done = true
      const bod = JSON.parse(`[${this.body}]`)
      // console.log(bod)
      this.push({type: 'start', payload: this.options.name})
      for (var b of bod) {
        this.push({type: 'chunk', payload: b})
      }
      this.push({type: 'end', payload: this.options.name})
      this.push({type: 'finished'})
      this.end()
    }

  }

}
