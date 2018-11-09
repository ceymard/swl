import { Sink, Chunk, register } from '../pipeline'
import * as s from 'slz'


@register('pick')
export class Pick extends Sink(s.array(s.object().or(s.string()))) {
  help = `Pick properties from records`

  regexps: RegExp[] = []
  props: Set<string|number> = new Set()

  async init() {
    for (var p of this.params) {
      if (typeof p === 'string' || typeof p === 'number')
        this.props.add(p)
      else if (p instanceof RegExp)
        this.regexps.push(p)
    }
  }

  async onData(chunk: Chunk.Data) {

    var p = chunk.payload
    var result: any = {}

    for (var x in p) {
      if (this.props.has(x))
        result[x] = p[x]
      else {
        for (var r of this.regexps) {
          if (r.test(x))
            result[x] = p[x]
        }
      }
    }

    this.send(Chunk.data(chunk.collection, result))

  }
}
