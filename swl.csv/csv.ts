import {Adapter, CollectionStartPayload} from 'swl'
import * as fs from 'fs'
import * as stringify from 'csv-stringify'
import * as yup from 'yup'

export interface CsvAdapterOptions {
  encoding?: string
  header?: boolean
  delimiter?: string
}

export class CsvAdapter extends Adapter<CsvAdapterOptions> {

  schema = yup.object({
    encoding: yup.string().default('utf-8'),
    header: yup.boolean().default(true),
    delimiter: yup.string().default(',')
  })

  is_source = false
  out: stringify.Stringifier | null = null

  async onCollectionStart({name}: CollectionStartPayload) {
    var file = fs.createWriteStream(this.uri.replace('%col', name), {
      flags: 'w',
      // encoding: this.options.encoding || 'utf-8'
    })

    this.out = stringify({
      header: this.options.header || true,
      delimiter: this.options.delimiter || ','
    })

    const opt = this.schema.cast(this.options)

    this.setOptions({
      encoding: 'utf-8',
      header: true,
      delimiter: ','
    })
    console.log(opt)

    this.out!.pipe(file)
      // .pipe(file)
  }

  async onCollectionEnd() {
    this.out!.end()
  }

  async onChunk(chk: any) {
    this.out!.write(chk)
  }

}

CsvAdapter.register('csv')
  .registerMime('text/csv')
