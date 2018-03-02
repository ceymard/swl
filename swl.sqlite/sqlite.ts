
import {Source, PipelineEvent, Sink, register_source, URI_AND_OBJ, register_sink, URI, sources, ChunkIterator, Chunk} from 'swl'
import * as S from 'better-sqlite3'


sources.add(function sqlite(opts: any, rest: string) {

  const [file, sources] = URI_AND_OBJ.tryParse(rest)

  return async function *sqlite_reader(upstream: ChunkIterator): ChunkIterator {
    yield* upstream

    const db = new S(file, opts)
    // Throw error if db doesn't exist ! (unless option specifically allows for that)

    var keys = Object.keys(sources)
    if (keys.length === 0) {
      // Auto-detect *tables* (not views)
      const st = db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
      keys = st.all().map(k => k.name)
    }

    for (var colname of keys) {
      var val = sources[colname]

      var sql = typeof val !== 'string' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      var stmt = db.prepare(sql)

      yield Chunk.start(colname)
      for (var s of (stmt as any).iterate()) {
        yield Chunk.data(s)
      }
    }

  }

}, '.db', '.sqlite3', '.sqlite')


/**
 * SQlite doesn't speak all the data values that we may have
 */
export function coerce(value: any) {
  const typ = typeof value
  if (value === null || typ === 'string' || typ === 'number' || value instanceof Buffer) {
    return value
  }
  if (typ === 'boolean')
    return value ? 'true' : 'false'
  if (value === undefined)
    return null

  if (value instanceof Date)
    return value.toUTCString()
  if (Array.isArray(value))
    return value.join(', ')

  // console.log(value)
  return value.toString()
  // console.log(value)
  // throw new Error(`Unknown type for value: ${value}`)
}



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

        stmt.run(...columns.map(c => coerce(payload[c])))
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
