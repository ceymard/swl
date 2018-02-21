import {StreamSink, StreamSource, register_sink, URI_WITH_OPTS, make_write_creator, register_source, make_read_creator} from 'swl'

import * as stringify from 'csv-stringify'
import * as parse from 'csv-parse'
import * as y from 'yup'

export interface CsvAdapterOptions {
  encoding?: string
  header?: boolean
  delimiter?: string
}

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
    delimiter: y.string().default(','),
    auto_parse: y.boolean().default(true)
  })
  options = this.schema.cast(this.options)

  codec() {
    return parse(this.options)
  }

}

register_source(async (opts: any, rest: string) => {
  const [uri, options] = URI_WITH_OPTS.tryParse(rest)
  options.encoding = 'utf-8'
  // console.log(uri, options)
  return new CsvSource(opts, await make_read_creator(uri, options || {}))
}, 'csv', '.csv')