
import {Source, PipelineEvent, PipelineComponent, register_source} from 'swl'
import * as S from 'better-sqlite3'


export type Selector = boolean | string


export class SqliteSource extends Source {

  constructor(public filename: string, public options = {}, public sources: {[name: string]: Selector} = {}) {
    super()
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    const db = new S(this.filename, this.options)

    for (var colname in this.sources) {
      var val = this.sources[colname]

      var sql = typeof val === 'boolean' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      var stmt = db.prepare(sql)

      yield this.start(colname)
      for (var s of stmt.iterate()) {
        yield this.data(s)
      }
    }
  }

}

register_source((parse: string) => {
  return new SqliteSource(parse, {}, {})
}, 'sqlite')


export class SqliteSink extends PipelineComponent {

  mode: 'insert' | 'upsert' | 'update' = 'insert'
  db!: S

  constructor(public filename: string, public options: any = {}) {
    super()
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
            CREATE TABLE IF NOT EXISTS ${table}(
              ${columns.map((c, i) => `${c} ${types[i]}`).join(', ')}
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