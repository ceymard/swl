
import { sinks, ChunkIterator, Chunk } from '../pipeline'
import * as y from 'yup'

sinks.add(
  y.object({}),
  function js(opts, rest) {

    var res: Function = eval(rest)

    return async function* (upstream: ChunkIterator): ChunkIterator {
      // yield* upstream
      var collection = ''
      var i = 0
      for await (var ch of upstream) {
        if (ch.type === 'start') {
          collection = ch.name
          i = 0
          yield ch
        } else if (ch.type === 'data') {
          i++
          yield Chunk.data(res(ch.payload, collection, i))
        } else yield ch
      }
    }
  }
)
