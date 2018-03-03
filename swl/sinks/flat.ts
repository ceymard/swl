import { ChunkIterator, sinks, Chunk } from 'swl'
import { flatten as f, unflatten as u } from 'flat'
import * as y from 'yup'


sinks.add(
  y.object(),
  function flatten(opts) {

    return async function *flatten(upstream: ChunkIterator): ChunkIterator {
      for await (var ch of upstream) {
        if (ch.type === 'data') {
          yield Chunk.data(f(ch.payload, opts))
        } else yield ch
      }
    }

  }
)

sinks.add(
  y.object(),
  function unflatten(opts) {

    return async function *unflatten(upstream: ChunkIterator): ChunkIterator {
      for await (var ch of upstream) {
        if (ch.type === 'data') {
          yield Chunk.data(u(ch.payload, opts))
        } else yield ch
      }
    }

  }
)
