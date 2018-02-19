import {PipelineComponent} from 'swl'

import * as fs from 'fs'
import * as stringify from 'csv-stringify'
import * as yup from 'yup'

export interface CsvAdapterOptions {
  encoding?: string
  header?: boolean
  delimiter?: string
}

export class CsvOutput extends PipelineComponent {

  schema = yup.object({
    encoding: yup.string().default('utf-8'),
    header: yup.boolean().default(true),
    delimiter: yup.string().default(',')
  })

  is_source = false
  output!: stringify.Stringifier

  constructor(public options: CsvAdapterOptions, public uri: string) {
    super()
  }

  async onstart(name: string) {
    var file = fs.createWriteStream(this.uri.replace('%col', name), {
      flags: 'w',
      // encoding: this.options.encoding || 'utf-8'
    })

    this.output = stringify({
      header: this.options.header || true,
      delimiter: this.options.delimiter || ','
    })

    this.output.pipe(file)
  }

  async onstop() {
    this.output.end()
  }

  async ondata(chk: any) {
    this.output.write(chk)
  }

}

// CsvAdapter.register('csv')
//   .registerMime('text/csv')
