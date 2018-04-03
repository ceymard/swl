import { URI_WITH_OPTS, make_write_creator, make_read_creator, sources, y, ChunkIterator, Chunk, StreamWrapper, sinks} from 'swl'

import * as stringify from 'csv-stringify'
import * as parse from 'csv-parse'

export interface CsvAdapterOptions {
  encoding?: string
  header?: boolean
  delimiter?: string
}

sources.add(
  y.object({
    columns: y.boolean().default(true),
    delimiter: y.string().default(';'),
    auto_parse: y.boolean().default(true)
  }),
  async function csv(opts, rest) {
    const [uri, source_options] = URI_WITH_OPTS.tryParse(rest)
    source_options.encoding = 'utf-8'
    const sources = await make_read_creator(uri, source_options || {})

    return async function *csv(upstream: ChunkIterator): ChunkIterator {
      yield* upstream

      for await (var src of sources) {
        yield Chunk.start(src.collection)
        const stream = new StreamWrapper(src.source.pipe(parse(opts)))
        var value
        while ( (value = await stream.read()) !== null ) {
          yield Chunk.data(value)
        }
      }
    }
}, 'csv', '.csv')


sinks.add(
  y.object({
    encoding: y.string().default('utf-8'),
    header: y.boolean().default(true),
    delimiter: y.string().default(';')
  }),
  async function csv(opts, rest) {
    const [uri, options] = URI_WITH_OPTS.tryParse(rest)

    return async function *csv(upstream: ChunkIterator): ChunkIterator {
      var w = await make_write_creator(uri, options)
      var file: StreamWrapper<NodeJS.WritableStream>

      for await (var chk of upstream) {
        if (chk.type === 'start') {
          var end = await w(chk.name)
          var st = stringify(opts)
          st.pipe(end)
          file = new StreamWrapper(st)
        } else if (chk.type === 'data') {
          await file!.write(chk.payload)
        } else yield chk
      }
    }
  }, 'csv', '.csv'
)
