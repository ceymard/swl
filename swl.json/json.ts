import {Source, PipelineEvent, register_source} from 'swl'

var id = 0
export class InlineJson extends Source {

  objects: any[]

  constructor(public options: any, public body: string) {
    super(options)
    this.objects = JSON.parse(`[${body}]`)
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    yield this.start(Object.keys(this.options)[0] || `json-${id++}`)
    for (var o of this.objects) {
      yield this.data(o)
    }
  }

}

register_source(async (opts: any, str: string) => {
  return new InlineJson(opts, str)
}, 'inline-json')