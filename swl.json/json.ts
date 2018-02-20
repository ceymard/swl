import {Source, PipelineEvent} from 'swl'


var id = 0
export class InlineJson extends Source {

  objects: any[]

  constructor(public options: any, public body: string) {
    super()
    this.objects = JSON.parse(`[${body}]`)
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    yield this.event('start', Object.keys(this.options)[0] || `json-${id++}`)
    for (var o of this.objects) {
      yield this.event('data', o)
    }
  }

}

