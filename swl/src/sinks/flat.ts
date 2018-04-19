import { ChunkIterator, transformers, Chunk } from '../pipeline'
import { flatten as f, unflatten as u } from 'flat'
import * as y from 'yup'


transformers.add(
`Flatten deep-nested properties to a simple object`,
  y.object(),
  null,
  function flatten(opts) {

    return async function *flatten(upstream: ChunkIterator): ChunkIterator {
      for await (var ch of upstream) {
        if (ch.type === 'data') {
          yield Chunk.data(f(ch.payload, opts))
        } else yield ch
      }
    }

  }, 'flatten'
)

transformers.add(
`Unflatten an object to a deep nested structure`,
  y.object(),
  null,
  function unflatten(opts) {

    return async function *unflatten(upstream: ChunkIterator): ChunkIterator {
      for await (var ch of upstream) {
        if (ch.type === 'data') {
          yield Chunk.data(u(ch.payload, opts))
        } else yield ch
      }
    }

  }, 'unflatten'
)
