import {Adapter, CollectionStartPayload} from 'swl'
import {Writable} from 'stream'
import * as fs from 'fs'
import * as stringify from 'csv-stringify'

export interface CsvAdapterOptions {
  encoding?: string
  header?: boolean
  delimiter?: string
}

export class CsvAdapter extends Adapter<CsvAdapterOptions> {

  is_source = false
  out: Writable | null = null

  async onCollectionStart({name}: CollectionStartPayload) {
    var file = fs.createWriteStream(this.uri.replace('%col', name), {
      flags: 'w',
      // encoding: this.options.encoding || 'utf-8'
    })

    this.out = stringify({
      header: this.options.header || true,
      delimiter: this.options.delimiter || ','
    })

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
