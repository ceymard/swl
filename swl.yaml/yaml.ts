
import {Sequence, OPT_OBJECT, URI, s, Chunk, Source, register, ParserType, Sink } from 'swl'
import { readFileSync } from 'fs'
import { safeLoad } from 'js-yaml'


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
    const parsed: object = safeLoad(contents, { filename: this.filename, }) as any

    for (const [col, cts] of Object.entries(parsed)) {
      for (var obj of cts) {
        await this.send(Chunk.data(col, obj))
      }
    }
    // console.log(parsed)

    // var sources = this.sources
    // var keys = Object.keys(sources||{})

    // for (var colname of keys) {
      // var val = sources[colname]

      // var sql = typeof val !== 'string' ? `SELECT * FROM "${colname}"`
      // : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      // : val

      // var stmt = this.db.prepare(sql)

      // this.info(`Started ${colname}`)
      // var iterator = (stmt as any).iterate() as IterableIterator<any>
      // for (var s of iterator) {
      //   if (this.uncoerce) {
      //     var s2: any = {}
      //     for (var x in s)
      //       s2[x] = uncoerce(s[x])
      //     s = s2
      //   }

      //   await this.send(Chunk.data(colname, s))
      // }
    // }
    this.info('done')

  }

}

// export const YAML_SINK_OPTIONS = s.object({
//   truncate: s.boolean(false),
//   drop: s.boolean(false),
//   pragma: s.boolean(true)
// })

// export const YAML_SINK_BODY = URI

// @register('sqlite', '.db', '.sqlite', '.sqlite3')
// export class SqliteSink extends Sink<
//   s.BaseType<typeof YAML_SINK_OPTIONS>,
//   ParserType<typeof URI>
// > {

//   help = `Write to a SQLite database`
//   options_parser = YAML_SINK_OPTIONS
//   body_parser = URI

//   mode = 'insert' as 'insert' | 'upsert' | 'update'

//   table = ''
//   db!: S

//   pragmas: {[name: string]: any} = {}
//   columns: string[] = []
//   stmt: {run(...a:any): any} = undefined!

//   async init() {
//     const db = new S(await this.body, {})
//     this.db = db

//     if (this.options.pragma) {
//       this.pragmas.journal_mode = db.pragma('journal_mode', true)
//       this.pragmas.synchronous = db.pragma('synchronous', true)
//       this.pragmas.locking_mode = db.pragma('locking_mode', true)

//       db.pragma('journal_mode = off')
//       db.pragma('synchronous = 0')
//       db.pragma('locking_mode = EXCLUSIVE')
//     }

//     db.exec('BEGIN')
//   }

//   async error(e: any) {
//     this.db.exec('ROLLBACK')
//     throw e
//   }

//   async end() {
//     this.db.exec('COMMIT')
//   }

//   async final() {
//     if (this.options.pragma) {
//       for (var x in this.pragmas) {
//         this.db.pragma(`${x} = ${this.pragmas[x]}`)
//       }
//     }
//     if (this.db) this.db.close()
//   }


//   /**
//    * Create the table, truncate it or drop it if necessary
//    */
//   async onCollectionStart(start: Chunk.Data) {

//     var sql = ''
//     var table = start.collection
//     var payload = start.payload
//     var columns = Object.keys(payload)
//     this.columns = columns

//     var types = columns.map(c => typeof payload[c] === 'number' ? 'int'
//     : payload[c] instanceof Buffer ? 'blob'
//     : 'text')

//     if (this.options.drop) {
//       sql = `DROP TABLE IF EXISTS "${table}"`
//       this.info(sql)
//       this.db.exec(sql)
//     }

//     // Create if not exists ?
//     // Temporary ?
//     sql = `CREATE TABLE IF NOT EXISTS "${table}" (
//         ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
//       )`
//     this.info(sql)
//     this.db.exec(sql)

//     if (this.mode === 'insert') {
//       const sql = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
//       values (${columns.map(c => '?').join(', ')})`
//       // console.log(sql)
//       this.stmt = this.db.prepare(sql)
//     }

//     else if (this.mode === 'upsert')
//       // Should I do some sub-query thing with coalesce ?
//       // I would need some kind of primary key...
//       this.stmt = this.db.prepare(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
//         values (${columns.map(c => '?').join(', ')})`)


//     if (this.options.truncate) {
//       sql = `DELETE FROM "${table}"`
//       this.info(sql)
//       this.db.exec(sql)
//     }
//   }

//   async onData(data: Chunk.Data) {
//     this.stmt.run(...this.columns.map(c => coerce(data.payload[c])))
//   }
// }


// const re_date = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?:\d{2}(?:\.\d{3}Z?)))?$/
// const re_number = /^\d+(\.\d+)?$/
// const re_boolean = /^true|false$/i

// export function uncoerce(value: any) {
//   if (value && (value[0] === '{' || value[0] === '[')) {
//     try {
//       return JSON.parse(value)
//     } catch (e) {
//       return value
//     }
//   }

//   if (typeof value === 'string') {
//     var trimmed = value.trim().toLowerCase()

//     if (trimmed.match(re_date)) {
//       return new Date(trimmed)
//     }

//     if (trimmed.match(re_boolean)) {
//       return trimmed.toLowerCase() === 'true'
//     }

//     if (trimmed.match(re_number)) {
//       return parseFloat(trimmed)
//     }

//     if (trimmed === 'null')
//       return null
//   }

//   return value
// }


// export function coerce(value: any) {
//   const typ = typeof value
//   if (value === null || typ === 'string' || typ === 'number' || value instanceof Buffer) {
//     return value
//   }
//   if (typ === 'boolean')
//     return value ? 'true' : 'false'
//   if (value === undefined)
//     return null

//   if (value instanceof Date) {
//     return (new Date(value.valueOf() - (value.getTimezoneOffset() * 60000))).toISOString()
//   } //if (Array.isArray(value))
//     //return value.join(', ')

//   return JSON.stringify(value)
// }
