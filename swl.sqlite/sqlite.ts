
import {Sequence, OPT_OBJECT, URI, y, sources, ChunkIterator, Chunk, sinks} from 'swl'
import * as S from 'better-sqlite3'

sources.add(
`Read an SQLite database`,
  y.object(),
  Sequence(URI, OPT_OBJECT).name`SQlite Options`,
  function sqlite(opts, [file, sources]) {

  return async function *sqlite_reader(upstream: ChunkIterator): ChunkIterator {
    yield* upstream

    const db = new S(file, opts)
    // Throw error if db doesn't exist ! (unless option specifically allows for that)

    var keys = Object.keys(sources||{})
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

}, 'sqlite', '.db', '.sqlite3', '.sqlite')


sinks.add(
`Write to a SQLite database`,
  y.object({
    truncate: y.boolean().default(false).label('Truncate tables before loading'),
    drop: y.boolean().default(false).label('Drop tables'),
    pragma: y.boolean().default(true).label('Set pragmas before and revert them')
  }),
  URI,
  function sqlite(opts, file) {
    const mode: 'insert' | 'upsert' | 'update' = 'insert'

    async function* run(db: S, upstream: ChunkIterator): ChunkIterator {

      var table: string = ''
      var columns: string[] = []
      var start = false
      var stmt: any

      for await (var ev of upstream) {
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

            if (opts.drop) {
              db.exec(`DROP TABLE IF EXISTS "${table}"`)
            }

            // Create if not exists ?
            // Temporary ?
            db.exec(`
              CREATE TABLE IF NOT EXISTS "${table}" (
                ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
              )
            `)

            if (mode === 'insert') {
              const sql = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
              values (${columns.map(c => '?').join(', ')})`
              // console.log(sql)
              stmt = db.prepare(sql)
            }
            else if (mode === 'upsert')
              // Should I do some sub-query thing with coalesce ?
              // I would need some kind of primary key...
              stmt = db.prepare(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
                values (${columns.map(c => '?').join(', ')})`)


            if (opts.truncate) {
              db.exec(`DELETE FROM "${table}"`)
            }
            start = false
          }

          stmt.run(...columns.map(c => coerce(payload[c])))

        } else if (ev.type === 'exec') {
          // await (this as any)[ev.method](ev.options, ev.body)
        }
      }

    }

    return async function *sqlite_reader(upstream: ChunkIterator): ChunkIterator {

      const db = new S(file, {})
      var pragma_journal: string = 'delete'
      var sync: number = 0
      var locking_mode: string = 'exclusive'


      if (opts.pragma) {
        pragma_journal = db.pragma('journal_mode', true)
        sync = db.pragma('synchronous', true)
        locking_mode = db.pragma('locking_mode', true)

        db.pragma('journal_mode = off')
        db.pragma('synchronous = 0')
        db.pragma('locking_mode = EXCLUSIVE')
      }

      db.exec('BEGIN')
      try {
        yield* run(db, upstream)
        db.exec('COMMIT')
      } catch (e) {
        db.exec('ROLLBACK')
        throw e
      } finally {
        db.pragma(`journal_mode = ${pragma_journal}`)
        db.pragma(`synchronous = ${sync}`)
        db.pragma(`locking_mode = ${locking_mode}`)
      }

      db.close()
    }
  },
  'sqlite', '.db', '.sqlite3', '.sqlite'
)


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

  return JSON.stringify(value)
}
