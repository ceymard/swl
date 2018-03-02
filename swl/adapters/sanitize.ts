
import { Sink, PipelineEvent } from './adapter'
import { register_sink } from 'swl/register'
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

/**
 * Sanitize string input
 */
export class Sanitizer extends Sink {

  schema = y.object({
    columns: y.boolean().default(true),
    collections: y.boolean().default(true),
    values: y.boolean().default(false),
  })

  options = this.schema.cast(this.options)

  async *process(): AsyncIterableIterator<PipelineEvent> {
    var o = this.options
    for await (var ev of this.upstream()) {
      if (ev.type === 'start' && o.collections) {
        yield this.start(sanitize(ev.name))
      } else if (ev.type ==='data' && (o.columns || o.values)) {
        var p = ev.payload
        var n: any = {}
        for (var x in p) {
          n[o.columns ? sanitize(x) : x] = o.values && typeof p[x] === 'string' ? sanitize(p[x]) : p[x]
        }
        yield this.data(n)
      } else {
        yield ev
      }
    }
  }

}

register_sink(async (opts: any, src: string) => {
  return new Sanitizer(opts)
}, 'sanitize')