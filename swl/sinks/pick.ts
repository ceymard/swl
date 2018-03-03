import { ChunkIterator, sinks, Chunk, ARRAY_CONTENTS } from 'swl'
import * as y from 'yup'

sinks.add(
  y.object(),
  function pick(opts, rest) {
    const def = ARRAY_CONTENTS.tryParse(rest)

    return async function *pick(upstream: ChunkIterator): ChunkIterator {
      for await (var ev of upstream) {
        if (ev.type === 'data') {
          var obj: any = {}
          var p = ev.payload
          for (var d of def) {
            if (typeof d === 'string' || typeof d === 'number') {
              obj[d] = p[d]
            } if (d instanceof RegExp) {
              for (var x in p) {
                if (x.match(d))
                  obj[x] = p[x]
              }
            }
          }
          yield Chunk.data(obj)
        } else yield ev

    }
  }
)