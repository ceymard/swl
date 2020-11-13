
import {Sequence, OPT_OBJECT, URI, s, Chunk, Source, register, ParserType, Sink } from 'swl'
import { readFileSync, writeFileSync } from 'fs'
import { safeDump, safeLoad } from 'js-yaml'


const YAML_SOURCE_OPTIONS = s.object({
  collections: s.boolean(true)
})

const YAML_BODY = Sequence(URI, OPT_OBJECT).name`SQlite Options`

@register('yaml', 'yml', '.yaml', '.yml')
export class YamlSource extends Source<
  s.BaseType<typeof YAML_SOURCE_OPTIONS>,
  ParserType<typeof YAML_BODY>
  >
{
  help = `Read an SQLite database`

  options_parser = YAML_SOURCE_OPTIONS
  body_parser = YAML_BODY

  // ????
  collections!: boolean
  filename!: string
  // sources!: {[name: string]: boolean | string}

  async init() {
    this.filename = await this.body[0]
    this.collections = this.options.collections
  }

  async end() {

    // this.db.close()
  }

  async emit() {
    // console.log(this.filename)
    const contents = readFileSync(this.filename, 'utf-8')
    const parsed: object | any[] = safeLoad(contents, { filename: this.filename, }) as any


    for (const [col, cts] of Object.entries(parsed)) {
      for (var obj of cts) {
        await this.send(Chunk.data(col, obj))
      }
    }
    this.info('done')

  }

}

export const YAML_SINK_OPTIONS = s.object({
  collections: s.boolean(true)
})

export const YAML_SINK_BODY = URI

@register('yaml', 'yml', '.yaml', '.yml')
export class SqliteSink extends Sink<
  s.BaseType<typeof YAML_SINK_OPTIONS>,
  ParserType<typeof URI>
> {

  help = `Write to a Yaml file`
  options_parser = YAML_SINK_OPTIONS
  body_parser = URI

  filename!: string
  acc: {[name: string]: any[]} = {}
  collection: any[] = []

  async init() {
    this.filename = (await this.body)
  }

  // async end() {
  // }

  async final() {
    writeFileSync(this.filename, safeDump(this.acc, { indent: 2, lineWidth: 120, noArrayIndent: true }), 'utf-8')
  }


  /**
   * Create the table, truncate it or drop it if necessary
   */
  async onCollectionStart(start: Chunk.Data) {
    this.collection = this.acc[start.collection] = []
  }

  async onData(data: Chunk.Data) {
    this.collection.push(data.payload)
  }
}
