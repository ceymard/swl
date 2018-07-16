import { ChunkIterator, transformers, Chunk } from '../pipeline'
import * as y from 'yup'


// var _cache = {} as {[str: string]: string}
function san(str: string): string {
  // const cached = _cache[str]
  // if (typeof cached !== 'undefined') return cached
  // Remove accents
  const res = str.normalize('NFD').replace(/[\u0300-\u036f]/g, "")
  .replace(/[-\.:;\n \t\n\r+_]+/gm, '_')
  // only keep ascii characters, numbers, and a few useful characters like punctutation
    .replace(/[^\w-0-9_/\\!?,:; \s\n\{\}]/gm, '')
    .trim()
    .toLowerCase()
  // _cache[str] = res
  return res
}

transformers.add(
`Sanitize object input by removing non-ascii characters`,
  y.object({
    columns: y.boolean().default(true),
    collections: y.boolean().default(true),
    values: y.boolean().default(false),
  }),
  null,
  function sanitize(opts) {
    return async function *sanitize(upstream: ChunkIterator): ChunkIterator {
      const column_cache = {} as  {[s: string]: string}
      const ocolumns = !!opts.columns
      const ovalues = !!opts.values
      for await (var ev of upstream) {
        if (ev.type === 'start' && opts.collections) {
          yield Chunk.start(san(ev.name))
        } else if (ev.type ==='data' && (ocolumns || ovalues)) {
          var p = ev.payload
          var n: any = {}
          for (var x in p) {
            n[ocolumns ? (column_cache[x] = column_cache[x] || san(x)) : x] = ovalues && typeof p[x] === 'string' ? san(p[x]) : p[x]
          }
          yield Chunk.data(n)
        } else {
          yield ev
        }
      }
    }
  },
  'sanitize'
)
