import {StreamSink, StreamSource} from 'swl'

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

  is_source = false
  // output!: stringify.Stringifier

  async codec() {
    const opts = this.schema.cast(this.options)
    return stringify({
      header: opts.header,
      delimiter: opts.delimiter
    })
  }

}


export class CsvSource extends StreamSource {

}