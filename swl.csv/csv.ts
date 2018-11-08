import { make_write_creator, make_read_creator, Chunk, StreamWrapper, register, Source, s, Sink } from 'swl'

import * as stringify from 'csv-stringify'

const parse = require('csv-parser')

const CSV_SOURCE_OPTIONS = s.tuple(
  s.string(), // the URI
  s.object({
    columns: s.boolean(true).help `The first row are columns`,
    separator: s.string(';'),
    auto_parse: s.boolean(true),
    encoding: s.string('utf-8')
  })
)

@register('csv', '.csv')
export class CsvSource extends Source(CSV_SOURCE_OPTIONS) {
  help = `Read csv files`

  uri = this.params[0]
  options = this.params[1]

  async emit() {
    const sources = await make_read_creator(this.uri, this.options || {})

    for await (var src of sources) {
      const stream = new StreamWrapper(src.source.pipe(parse(this.options)))
      var value: any
      while ( (value = await stream.read()) !== null ) {
        await this.send(Chunk.data(src.collection, value))
      }
    }

  }

}


@register('csv', '.csv')
export class CsvSink extends Sink(CSV_SOURCE_OPTIONS) {

  help = `Output to csv`

  file!: StreamWrapper<NodeJS.WritableStream>

  options = this.params[1]
  uri = this.params[0]

  async onCollectionStart(chk: Chunk.Data) {
    var w = await make_write_creator(await this.uri, Object.assign({}, this.options))
    var str = await w(chk.collection)
    var st = stringify(this.options)
    st.pipe(str.stream)
    this.file = new StreamWrapper(st)
  }

  async onCollectionEnd() {
    this.file.close()
  }

  async onData(chk: Chunk.Data) {
    await this.file.write(chk.payload)
  }
}
