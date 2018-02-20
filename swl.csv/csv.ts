import {StreamSink, StreamSource, register_sink, URI_WITH_OPTS, make_write_creator} from 'swl'

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

  async codec() {
    const opts = this.schema.cast(this.options)
    return stringify({
      header: opts.header,
      delimiter: opts.delimiter
    })
  }

}

register_sink(async (opts: any, str: string) => {
  const [uri, options] = URI_WITH_OPTS.tryParse(str)
  return new CsvOutput(opts, await make_write_creator(uri, options))
})


export class CsvSource extends StreamSource {

}