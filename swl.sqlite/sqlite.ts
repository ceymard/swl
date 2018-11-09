
import {URI, s, Chunk, Source, register, Sink } from 'swl'
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


@register('sqlite', 'sqlite3', '.db', '.sqlite', '.sqlite3')
export class SqliteSource extends Source(s.tuple(
  s.string(), // uri !
  s.object({uncoerce: s.boolean(false)}),
  s.array(s.indexed(
    s.boolean().or(s.string())
  ))
))
{
  help = `Read an SQLite database`

  // sources!: {[name: string]: boolean | string}

  db!: S

  filename = this.params[0]
  options = this.params[1]
  sources = this.params[2]

  async init() {
    this.db = new S(this.filename, {readonly: true, fileMustExist: true})

    this.db.function('coalesce_join', {
      varargs: true, deterministic: true, safeIntegers: true}, coalesce_join)
    this.db.function('cleanup', {
      varargs: false,
      deterministic: true,
      safeIntegers: true}, cleanup)
    this.db.function('counter', {
      varargs: false,
      deterministic: false,
      safeIntegers: true}, counter)
    this.db.function('reset_counter', {
      varargs: false,
      deterministic: false, safeIntegers: true}, reset_counter)
  }

  async end() {
    this.db.close()
  }

  async emit() {

    var sources = [] as {name: string, query: string}[]
    if (this.sources.length === 0) {
      // Auto-detect *tables* (not views)
      // If no sources are specified, all the tables are outputed.
      const st = this.db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
        .pluck<string>()
      for (var table_name of st.all()) {
        sources.push({name: table_name, query: `select * from "${table_name}"`})
      }
    } else {
      for (var s of this.sources) {
        for (var k in s) {
          var q = s[k]
          sources.push({
            name: k,
            query: typeof q === 'boolean' ? `select * from "${k}"` : q
          })
        }
      }
    }

    for (var src of sources) {
      var name = src.name

      var stmt = this.db.prepare(src.query)

      this.info(`Started ${name}`)
      var iterator = (stmt as any).iterate() as IterableIterator<any>
      for (var it of iterator) {
        if (this.options.uncoerce) {
          var s2: any = {}
          for (var x in it)
            s2[x] = uncoerce(it[x])
          it = s2
        }

        await this.send(Chunk.data(name, it))
      }
    }
    this.info('done')

  }

}

export const SQLITE_SINK_OPTIONS = s.tuple(
  s.string(),
  s.object({
    truncate: s.boolean(false),
    drop: s.boolean(false),
    pragma: s.boolean(true)
  })
)

export const SQLITE_SINK_BODY = URI

@register('sqlite', '.db', '.sqlite', '.sqlite3')
export class SqliteSink extends Sink(SQLITE_SINK_OPTIONS) {

  help = `Write to a SQLite database`

  mode = 'insert' as 'insert' | 'upsert' | 'update'

  table = ''
  db!: S
  filename = this.params[0]
  options = this.params[1]

  pragmas: {[name: string]: any} = {}
  columns: string[] = []
  stmt: {run(...a:any): any} = undefined!

  async init() {
    const db = new S(this.filename, {})
    this.db = db

    if (this.options.pragma) {
      this.pragmas.journal_mode = db.pragma('journal_mode', true)
      this.pragmas.synchronous = db.pragma('synchronous', true)
      this.pragmas.locking_mode = db.pragma('locking_mode', true)

      db.pragma('journal_mode = off')
      db.pragma('synchronous = 0')
      db.pragma('locking_mode = EXCLUSIVE')
    }

    db.exec('BEGIN')
  }

  async error(e: any) {
    this.db.exec('ROLLBACK')
    throw e
  }

  async end() {
    this.db.exec('COMMIT')
  }

  async final() {
    if (this.options.pragma) {
      for (var x in this.pragmas) {
        this.db.pragma(`${x} = ${this.pragmas[x]}`)
      }
    }
    if (this.db) this.db.close()
  }


  /**
   * Create the table, truncate it or drop it if necessary
   */
  async onCollectionStart(start: Chunk.Data) {

    var sql = ''
    var table = start.collection
    var payload = start.payload
    var columns = Object.keys(payload)
    this.columns = columns

    var types = columns.map(c => typeof payload[c] === 'number' ? 'int'
    : payload[c] instanceof Buffer ? 'blob'
    : 'text')

    if (this.options.drop) {
      sql = `DROP TABLE IF EXISTS "${table}"`
      this.info(sql)
      this.db.exec(sql)
    }

    // Create if not exists ?
    // Temporary ?
    sql = `CREATE TABLE IF NOT EXISTS "${table}" (
        ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
      )`
    this.info(sql)
    this.db.exec(sql)

    if (this.mode === 'insert') {
      const sql = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
      values (${columns.map(c => '?').join(', ')})`
      // console.log(sql)
      this.stmt = this.db.prepare(sql)
    }

    else if (this.mode === 'upsert')
      // Should I do some sub-query thing with coalesce ?
      // I would need some kind of primary key...
      this.stmt = this.db.prepare(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
        values (${columns.map(c => '?').join(', ')})`)


    if (this.options.truncate) {
      sql = `DELETE FROM "${table}"`
      this.info(sql)
      this.db.exec(sql)
    }
  }

  async onData(data: Chunk.Data) {
    this.stmt.run(...this.columns.map(c => coerce(data.payload[c])))
  }
}


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

  if (value instanceof Date) {
    return value.toISOString()
  } if (Array.isArray(value))
    return value.join(', ')

  return JSON.stringify(value)
}
