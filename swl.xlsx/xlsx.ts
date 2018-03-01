
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


export function findTableRange() {

}


export class XlsxSource extends Source {

  constructor(
    public options: any,
    public sources: Sources,
    public obs: {[name: string]: Selector} = {}
  ) {
    super(options)
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    // yield this.start('zobi')
    // yield this.data({a: 1, b: 2})

    for await (const src of this.sources) {
      // console.log(src)
      const b = await get_stream(src.source)
      const w = XLSX.read(b, {cellHTML: false, cellText: false})
      // console.log(wp)

      for (var sname of w.SheetNames) {
        // console.log(sname)
        yield this.start(sname)
        const s = w.Sheets[sname]
        for (var obj of XLSX.utils.sheet_to_json(s)) {
          yield this.data(obj)
        }
        // console.log(s['A1'].v)
        // yield this.data({a: 1, b: 2})
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