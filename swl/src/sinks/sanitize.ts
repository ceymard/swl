import { Transformer, register, Chunk } from '../pipeline'
import * as s from '../slz'


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


const SANITIZE_OPTIONS = s.object({
  columns: s.boolean().default(true),
  collections: s.boolean().default(true),
  values: s.boolean().default(false)
})


@register('sanitize')
export class Sanitize extends Transformer<s.BaseType<typeof SANITIZE_OPTIONS>, string> {
  help = `Sanitize object input by removing non-ascii characters`

  options_parser = SANITIZE_OPTIONS
  body_parser = null
  column_cache = {} as  {[s: string]: string}

  async onData(chunk: Chunk.Data) {
    var ocolname = this.options.collections
    var ocolumns = this.options.columns
    var ovalues = this.options.values
    var column_cache = this.column_cache

    if (!this.options.columns && !this.options.values) {
      await this.send(chunk)
      return
    }

    var p = chunk.payload
    var n: any = {}
    for (var x in p) {
      n[ocolumns ? (column_cache[x] = column_cache[x] || san(x)) : x] = ovalues && typeof p[x] === 'string' ? san(p[x] as string) : p[x]
    }
    await this.send(Chunk.data(ocolname ? san(chunk.collection) : chunk.collection, n))
  }

}
