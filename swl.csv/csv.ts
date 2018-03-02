import { URI_WITH_OPTS, make_write_creator, make_read_creator, sources, y, ChunkIterator, Chunk, StreamWrapper} from 'swl'

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
}, '.csv')

/*
export class CsvOutput extends StreamSink {

  schema = y.object({
    encoding: y.string().default('utf-8'),
    header: y.boolean().default(true),
    delimiter: y.string().default(';')
  })

  options = this.schema.cast(this.options)

  async codec() {
    // const opts = this.schema.cast(this.options)
    return stringify({
      header: this.options.header,
      delimiter: this.options.delimiter
    })
  }

}

register_sink(async (opts: any, str: string) => {
  const [uri, options] = URI_WITH_OPTS.tryParse(str)
  return new CsvOutput(opts, await make_write_creator(uri, options))
}, 'csv', '.csv')


export class CsvSource extends StreamSource {

  schema = y.object({
    columns: y.boolean().default(true),
    delimiter: y.string().default(';'),
    auto_parse: y.boolean().default(true)
  })

  options = this.schema.cast(this.options)

  async nextSource(source: NodeJS.ReadableStream) {
    return source.pipe(parse(this.options))
  }

}

register_source(async (opts: any, rest: string) => {
  const [uri, options] = URI_WITH_OPTS.tryParse(rest)
  options.encoding = 'utf-8'
  // console.log(uri, options)
  return new CsvSource(opts, await make_read_creator(uri, options || {}))
}, 'csv', '.csv')
*/