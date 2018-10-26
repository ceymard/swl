
import {Sequence, OPT_OBJECT, URI, s, ChunkIterator, Chunk, Source, register, ParserType, Sink } from 'swl'
import * as S from 'better-sqlite3'

function coalesce_join(sep: string, ...a: (string|null|number)[]) {
  var r = []
  var l = a.length
  for (var i = 0; i < l; i++) {
    var b = ('' + (a[i]||'')).trim()
    if (b) r.push(b)
  }
  return r.join(sep)
}

function cleanup(str: string) {
  return (str||'').trim()
    .replace(/\s+/g, ' ')
    .replace(/\s*-\s*/g, '-')
}

var cache: {[name: string]: number} = {}
function counter(name: string, start: number) {
  var res = cache[name] = (cache[name] || start) + 1
  return res
}

function reset_counter(name: string) {
  delete cache[name]
}


const SQLITE_SOURCE_OPTIONS = s.object({
  uncoerce: s.boolean(false)
})

const SQLITE_BODY = Sequence(URI, OPT_OBJECT).name`SQlite Options`

@register('sqlite', 'sqlite3', '.db', '.sqlite', '.sqlite3')
export class SqliteSource extends Source<
  s.BaseType<typeof SQLITE_SOURCE_OPTIONS>,
  ParserType<typeof SQLITE_BODY>
  >
{
  help = `Read an SQLite database`

  options_parser = SQLITE_SOURCE_OPTIONS
  body_parser = SQLITE_BODY

  // ????
  uncoerce!: boolean
  filename!: string
  sources!: {[name: string]: boolean | string}

  db!: S

  async init() {
    this.filename = await this.body[0]
    this.sources = this.body[1]
    this.uncoerce = this.options.uncoerce

    this.db = new S(this.filename, {readonly: true, fileMustExist: true})

    this.db.register({
      name: 'coalesce_join',
      varargs: true, deterministic: true, safeIntegers: true}, coalesce_join)
    this.db.register({
      name: 'cleanup',
      varargs: false,
      deterministic: true,
      safeIntegers: true}, cleanup)
    this.db.register({
      name: 'counter',
      varargs: false,
      deterministic: false,
      safeIntegers: true}, counter)
    this.db.register({
      name: 'reset_counter',
      varargs: false,
      deterministic: false, safeIntegers: true}, reset_counter)
  }

  async end() {
    this.db.close()
  }

  async* emit(): ChunkIterator {
    var sources = this.sources
    var keys = Object.keys(sources||{})

    if (keys.length === 0) {
      // Auto-detect *tables* (not views)
      // If no sources are specified, all the tables are outputed.
      const st = this.db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
      keys = st.all().map(k => k.name)
    }

    for (var colname of keys) {
      var val = sources[colname]

      var sql = typeof val !== 'string' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      var stmt = this.db.prepare(sql)

      yield Chunk.info(this, `Started ${colname}`)
      yield Chunk.start(colname)
      for (var s of (stmt as any).iterate()) {
        if (this.uncoerce) {
          var s2: any = {}
          for (var x in s)
            s2[x] = uncoerce(s[x])
          s = s2
        }
        yield Chunk.data(s)
      }
    }
    yield Chunk.info(this, 'done')

  }

}

export const SQLITE_SINK_OPTIONS = s.object({
  truncate: s.boolean(false),
  drop: s.boolean(false),
  pragma: s.boolean(true)
})

export const SQLITE_SINK_BODY = URI

export class SqliteSink extends Sink<
  s.BaseType<typeof SQLITE_SINK_OPTIONS>,
  ParserType<typeof URI>
> {

  help = `Write to a SQLite database`
  options_parser = SQLITE_SINK_OPTIONS
  body_parser = URI

  mode = 'insert' as 'insert' | 'upsert' | 'update'

  table = ''
  db!: S

  async *handle(upstream: ChunkIterator): ChunkIterator {
    const db = new S(await this.body, {})
    this.db = db
    var pragma_journal: string = 'delete'
    var sync: number = 0
    var locking_mode: string = 'exclusive'

    if (this.options.pragma) {
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
      if (this.options.pragma) {
        db.pragma(`journal_mode = ${pragma_journal}`)
        db.pragma(`synchronous = ${sync}`)
        db.pragma(`locking_mode = ${locking_mode}`)
      }
    }

    db.close()
  }


}

sinks.add(
`Write to a SQLite database`,
  // y.object({
  //   truncate: y.boolean().default(false).label('Truncate tables before loading'),
  //   drop: y.boolean().default(false).label('Drop tables'),
  //   pragma: y.boolean().default(true).label('Set pragmas before and revert them')
  // }),
  URI,
  function sqlite(opts, file) {
    const mode: 'insert' | 'upsert' | 'update' = 'insert'
    const name = `${file}`
    const info = (message: string) => Chunk.info(name, message)

    async function* run(db: S, upstream: ChunkIterator): ChunkIterator {

      var table: string = ''
      var columns: string[] = []
      var start = false
      var stmt: any
      var sql = ''

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
              sql = `DROP TABLE IF EXISTS "${table}"`
              yield info(sql)
              db.exec(sql)
            }

            // Create if not exists ?
            // Temporary ?
            sql = `CREATE TABLE IF NOT EXISTS "${table}" (
                ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
              )`
            yield info(sql)
            db.exec(sql)

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
              sql = `DELETE FROM "${table}"`
              yield info(sql)
              db.exec(sql)
            }
            start = false
          }

          stmt.run(...columns.map(c => coerce(payload[c])))

        } else yield ev
      }

    }

    return async function *sqlite_writer(upstream: ChunkIterator): ChunkIterator {

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

const re_date = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?:\d{2}(?:\.\d{3}Z?)))?$/
const re_boolean = /^true|false$/i

export function uncoerce(value: any) {
  if (typeof value === 'string') {
    var trimmed = value.trim().toLowerCase()
    if (trimmed.match(re_date)) {
      return new Date(trimmed)
    }

    if (trimmed.match(re_boolean)) {
      return trimmed.toLowerCase() === 'true'
    }

    if (trimmed === 'null')
      return null
  }

  return value
}


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
