import {PipelineComponent, Source} from 'swl'


export interface JsonAdapterOptions {
  beautify?: boolean
  object?: boolean
}

export class JsonAdapter extends PipelineComponent {

  done = false
  first = true

}

var id = 0
export class InlineJson extends Source {

  objects: any[]

  constructor(public options: any, public body: string) {
    super()
    this.objects = JSON.parse(`[${body}]`)
  }

  async emit() {
    await this.send('start', Object.keys(this.options)[0] || `json-${id++}`)
    for (var o of this.objects) {
      await this.send('data', o)
    }
    this.send('end')
  }

}

export class JsonSink extends PipelineComponent {

}
