import { ChunkIterator, sinks, Chunk, ARRAY_CONTENTS } from 'swl'
import * as y from 'yup'


function sanitize(str: string): string {
  // Remove accents
  return str.normalize('NFD').replace(/[\u0300-\u036f]/g, "")
    // only keep ascii characters, numbers, and a few useful characters like punctutation
    .replace(/[^\w-_\/\\.!?,:; \s\n]/, '')
    .replace(/[\n\s+]/gm, '_')
    .trim()
    .toLowerCase()
}

sinks.add(
  y.object({
    columns: y.boolean().default(true),
    collections: y.boolean().default(true),
    values: y.boolean().default(false),
  }),
  function sanitize(opts) {
    return async function *sanitize(upstream: ChunkIterator): ChunkIterator {
      for await (var ev of upstream) {
        if (ev.type === 'start' && opts.collections) {
          yield Chunk.start(sanitize(ev.name))
        } else if (ev.type ==='data' && (opts.columns || opts.values)) {
          var p = ev.payload
          var n: any = {}
          for (var x in p) {
            n[opts.columns ? sanitize(x) : x] = opts.values && typeof p[x] === 'string' ? sanitize(p[x]) : p[x]
          }
          yield Chunk.data(n)
        } else {
          yield ev
        }
      }
    }
  }
)