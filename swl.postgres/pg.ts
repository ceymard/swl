
import {Source, PipelineEvent, Sink, register_source, URI_AND_OBJ, register_sink, URI} from 'swl'
import * as pg from 'pg'


export type Selector = boolean | string


export class PostgresSource extends Source {

  constructor(
    public options: any,
    public uri: string,
    public sources: {[name: string]: Selector} = {}
  ) {
    super(options)
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    console.log(this.uri)
    const uri = `postgres://${this.uri}`
    const db = new pg.Client(uri)
    await db.connect()
    // Throw error if db doesn't exist ! (unless option specifically allows for that)

    var keys = Object.keys(this.sources)
    if (keys.length === 0) {
      const tables = await db.query(`
        SELECT * FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'`)

      for (let res of tables.rows) {
        this.sources[res.table_name] = true
      }
      keys = Object.keys(this.sources)
    }
    // console.log(this.sources)

    for (var colname of keys) {
      var val = this.sources[colname]

      var sql = typeof val !== 'string' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      const result = await db.query(sql)

      yield this.start(colname)
      for (var s of result.rows) {
        yield this.data(s)
      }
    }

    await db.end()
  }

}

register_source(async (opts: any, parse: string) => {
  const [file, sources] = URI_AND_OBJ.tryParse(parse)
  // console.log(file, sources)
  return new PostgresSource(opts, file, sources)
}, 'postgres', 'pg')


export class PostgresSink extends Sink {

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

          if (this.mode === 'insert') {
            const sql = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
            values (${columns.map(c => '?').join(', ')})`
            // console.log(sql)
            stmt = this.db.prepare(sql)
          }
          else if (this.mode === 'upsert')
            // Should I do some sub-query thing with coalesce ?
            // I would need some kind of primary key...
            stmt = this.db.prepare(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
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
  return new PostgresSink(file, opts)
}, 'postgres', 'pg')
