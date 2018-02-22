import {Source, PipelineEvent, register_source, StreamSource, make_read_creator} from 'swl'

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


export class JsonSource extends StreamSource {

  tmp = ''
  pos = 0
  count = 0
  obj_start = 0

  async *handleChunk(chk: any): AsyncIterableIterator<PipelineEvent> {
    const st: string = chk instanceof Buffer ? chk.toString('utf-8') : chk as string
    var in_string = false
    var in_obj = false

    this.tmp += st
    var pos = this.pos, tmp = this.tmp, count = this.count
    for (; pos < tmp.length; pos++) {
      if (!in_string) {
        if (tmp[pos] === '{') {
          in_obj = true
          if (count === 0)
            this.obj_start = pos
          count += 1
        } else if (tmp[pos] === '}') {
          count -= 1
          in_obj = false
          if (count === 0) {
            var obj = tmp.slice(this.obj_start, pos - this.obj_start + 2)
            // console.log(obj)
            yield this.data(JSON.parse(obj))
            // We remove the excess object and start again
            tmp = this.tmp = this.tmp.slice(pos + 1)
            pos = -1
          }
        } else if (in_obj && tmp[pos] === '"') {
          in_string = true
        }
      } else {
        if (in_obj && tmp[pos] === '"' && tmp[pos - 1] !== '\\') {
          in_string = false
        }
      }
    }
    this.pos = pos
    this.count = count
  }

}

register_source(async (opts: any, str: string) => {
  return new InlineJson(opts, str)
}, 'inline-json')


register_source(async (opts:any, str: string) => {
  return new JsonSource(opts, await make_read_creator(str.trim(), {}))
},
  'json',
  '.json',
  'text/json',
  'application/json',
  'application/javascript',
  'application/ld+json',
  'application/vnd.geo+json'
)