import {StreamSink, StreamSource, register_sink, URI_WITH_OPTS, make_write_creator, register_source, make_read_creator} from 'swl'

import * as stringify from 'csv-stringify'
import * as yup from 'yup'

export interface CsvAdapterOptions {
  encoding?: string
  header?: boolean
  delimiter?: string
}

export class CsvOutput extends StreamSink {

  schema = yup.object({
    encoding: yup.string().default('utf-8'),
    header: yup.boolean().default(true),
    delimiter: yup.string().default(';')
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

}

register_source(async (opts: any, rest: string) => {
  const [uri, options] = URI_WITH_OPTS.tryParse(rest)
  return new CsvSource(opts, await make_read_creator(uri, options || {}))
}, 'csv', '.csv')