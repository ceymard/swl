export const A = 5

/*
import {
  Source,
  PipelineEvent,
  // Sink,
  register_source,
  URI_AND_OBJ,
  make_read_creator,
  Sources
  // register_sink,
  // URI
} from 'swl'

import * as y from 'yup'

import * as XLSX from 'xlsx'


export type Selector = boolean | string


function get_stream(stream: NodeJS.ReadableStream): Promise<Buffer> {
  var accept: (b: Buffer) => void, reject: (e: any) => void
  const p = new Promise<Buffer>((_acc, _rej) => {
    accept = _acc
    reject = _rej
  })
  var buffers: Buffer[] = [];

  stream.on('data', function(data) { buffers.push(data); });
  stream.on('error', function (err) {
    reject(err)
  })
  stream.on('end', function() {
    var buffer = Buffer.concat(buffers);
    accept(buffer)
  });

  return p
}

const _l = ['', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
const columns: string[] = []

for (let i = 0; i < _l.length; i++) {
  for (let j = 1; j < _l.length; j++) {
    columns.push(_l[i] + _l[j])
  }
}

export class XlsxSource extends Source {

  schema = y.object({
    header: y.string()
  })
  options = this.schema.cast(this.options)

  constructor(
    options: any,
    public sources: Sources,
    public obs: {[name: string]: Selector} = {}
  ) {
    super(options)
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {

    for await (const src of this.sources) {
      // console.log(src)
      const b = await get_stream(src.source)
      const w = XLSX.read(b, {cellHTML: false, cellText: false})
      // console.log(wp)

      for (var sname of w.SheetNames) {
        yield this.start(sname)
        const s = w.Sheets[sname]

        // Find out if this sheet should be part of the extraction
        if (Object.keys(this.obs).length > 0 && !this.obs[sname])
          // If there was a specification of keys and this sheet name is not
          // one of it, then just continue to the next collection
          continue

        const re_range = /^([A-Z]+)(\d+):([A-Z]+)(\d+)$/
        const match = re_range.exec(s['!ref'] as string)
        if (!match) continue

        // We have to figure out the number of lines
        const lines = parseInt(match[4])

        // Then we want to find the header row. By default it should be
        // "A1", or the first non-empty cell we find
        const header: string[] = []
        for (var i = 1; i < columns.length; i++) {
          const cell = s[`${columns[i]}3`]
          if (!cell)
            break
          header.push(cell.v)
        }

        // Now that we've got the header, we just go on with the rest of the lines
        var not_found_count = 0
        for (var j = 4; j <= lines; j++) {
          var obj: {[name: string]: any} = {}
          var found = false
          for (i = 1; i < header.length + 1; i++) {
            const cell = s[`${columns[i]}${j}`]
            if (cell) {
              obj[header[i - 1]] = cell.v
              found = true
              not_found_count = 0
            }
          }

          if (found)
            yield this.data(obj)
          else {
            not_found_count++
            if (not_found_count > 5)
            // More than five empty rows in a row means we're at the end of the document.
              break
          }
        }
      }
      // console.log(b.length)
    }
  }

}

register_source(async (opts: any, parse: string) => {
  const [file, sources] = URI_AND_OBJ.tryParse(parse)
  // console.log(file, sources)
  return new XlsxSource(opts, await make_read_creator(file, {}), sources)
}, 'xlsx', '.xlsx', '.xlsb', '.ods')


/*
export class SqliteSink extends Sink {

  mode: 'insert' | 'upsert' | 'update' = 'insert'
  db!: S

  constructor(public filename: string, public options: any = {}) {
    super(options)
  }

  run(options: any, body: string) {
    this.db.exec(body)
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {
    var table: string = ''
    var columns: string[] = []
    var start = false
    var stmt: any

    this.db = new S(this.filename, {})

    this.db.exec('BEGIN')

    for await (var ev of this.upstream()) {
      if (ev.type === 'start') {
        start = true
        table = ev.name
      } else if (ev.type === 'data') {
        var payload = ev.payload

        // Check if we need to create the table
        if (start) {
          columns = Object.keys(payload)
          var types = columns.map(c => typeof payload[c] === 'number' ? 'real'
          : payload[c] instanceof Buffer ? 'blob'
          : 'text')
          // Create if not exists ?
          // Temporary ?
          this.db.exec(`
            CREATE TABLE IF NOT EXISTS "${table}" (
              ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
            )
          `)

          if (this.mode === 'insert')
            stmt = this.db.prepare(`INSERT INTO ${table}(${columns.map(c => `"${c}"`).join(', ')})
              values (${columns.map(c => '?').join(', ')})`)
          else if (this.mode === 'upsert')
            // Should I do some sub-query thing with coalesce ?
            // I would need some kind of primary key...
            stmt = this.db.prepare(`INSERT OR REPLACE INTO ${table}(${columns.map(c => `"${c}"`).join(', ')})
              values (${columns.map(c => '?').join(', ')})`)


          if (this.options.truncate) {
            this.db.exec(`DELETE FROM "${table}"`)
          }
          start = false
        }

        stmt.run(...columns.map(c => payload[c]))
      } else if (ev.type === 'exec') {
        await (this as any)[ev.method](ev.options, ev.body)
      }
    }
    this.db.exec('COMMIT')
  }

}

register_sink(async (opts: any, parse: string) => {
  const file = URI.tryParse(parse.trim())
  // console.log(file, sources)
  return new SqliteSink(file, opts)
}, 'sqlite', '.sqlite', 'sqlite3', '.db')
*/