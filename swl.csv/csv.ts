import { URI_WITH_OPTS, make_write_creator, make_read_creator, Chunk, StreamWrapper, register, Source, s, ParserType, Sink } from 'swl'

import * as stringify from 'csv-stringify'

const parse = require('csv-parser')

const CSV_SOURCE_OPTIONS = s.object({
  columns: s.boolean(true).help `The first row are columns`,
  separator: s.string(';'),
  auto_parse: s.boolean(true),
  name: s.string('')
})

@register('csv', '.csv')
export class CsvSource extends Source<
  s.BaseType<typeof CSV_SOURCE_OPTIONS>,
  ParserType<typeof URI_WITH_OPTS>
> {
  help = `Read csv files`
  options_parser = CSV_SOURCE_OPTIONS
  body_parser = URI_WITH_OPTS

  async init() {
    var source_options = this.body[1]
    source_options.encoding = source_options.encoding || 'utf-8'
  }

  async emit() {
    const sources = await make_read_creator(await this.body[0], this.body[1] || {}, this.options.name || undefined)

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
export class CsvSink extends Sink<
  s.BaseType<typeof CSV_SOURCE_OPTIONS>,
  ParserType<typeof URI_WITH_OPTS>
> {

  help = `Output to csv`
  options_parser = CSV_SOURCE_OPTIONS
  body_parser = URI_WITH_OPTS

  file!: StreamWrapper<NodeJS.WritableStream>

  async init() {
  }

  async onCollectionStart(chk: Chunk.Data) {
    var w = await make_write_creator(await this.body[0], Object.assign({}, this.options, this.body[1]))
    var str = await w(chk.collection)
    var st = stringify(this.body[1])
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
