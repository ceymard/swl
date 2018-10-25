import { ChunkIterator, Sink, Chunk, register } from '../pipeline'
import { ARRAY_CONTENTS } from 'clion'


@register('pick')
export class Pick extends Sink<{}, any[]> {
  help = `Pick properties from records`
  options_parser = null
  body_parser = ARRAY_CONTENTS

  regexps: RegExp[] = []
  props: Set<string|number> = new Set()

  async init() {
    for (var p of this.body) {
      if (typeof p === 'string' || typeof p === 'number')
        this.props.add(p)
      else if (p instanceof RegExp)
        this.regexps.push(p)
    }
  }

  async *onData(chunk: Chunk.Data): ChunkIterator {

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

    yield Chunk.data(result)

  }
}
