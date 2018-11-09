
import {Chunk, s, Sink, StreamWrapper, Source, register, open_tunnel} from 'swl'
import * as pg from 'pg'
import * as _ from 'csv-stringify'
const copy_from = require('pg-copy-streams').from

const PG_SRC_OPTIONS = s.tuple(
  s.string().then(s => open_tunnel(s)), // the URI
  s.object(), // the options
  s.array(s.indexed( // at last, the sources
    s.boolean()
    .or(s.string())
  ))
)

@register('pg', 'postgres')
export class PgSource extends Source(PG_SRC_OPTIONS) {
  help = `Read from a PostgreSQL database`

  // sources: {[name: string]: boolean | string} = {}
  uri = this.params[0]
  options = this.params[1]
  sources = this.params[2]

  db!: pg.Client

  async emit() {
    var uri = `postgres://${await this.uri}`

    const db = new pg.Client(uri)
    await db.connect()
    this.db = db

    var sources = [] as {name: string, query: string}[]

    if (this.sources.length === 0) {
      // Get the list of all the tables if we did not know them.
      const tables = await db.query(`
        SELECT * FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'`)

      for (let res of tables.rows) {
        sources.push({name: res.table_name, query: `select * from "${res.table_name}"`})
      }
    } else {
      for (var s of this.sources) {
        for (var k in s) {
          var q = s[k]
          sources.push({name: k, query: typeof q === 'boolean' ? `select * from "${k}"` : q})
        }
      }
    }

    for (var src of sources) {
      var val = src.name

      const result = await db.query(src.query)

      for (let row of result.rows) {
        await this.send(Chunk.data(val, row))
      }
    }

  }

  async end() {
    await this.db.end()
  }

}



const PG_SINK_OPTIONS = s.tuple(
  s.string().then(s => open_tunnel(s)),
  s.object({
    truncate: s.boolean(false).default(false).help`Truncate tables before loading`,
    notice: s.boolean(true).default(true).help`Show notices on console`,
    drop: s.boolean(false).default(false).help`Drop tables`,
    upsert: s.object({}).help`Upsert Column Name`
  })
)

@register('pg', 'postgres')
export class PgSink extends Sink(PG_SINK_OPTIONS) {

  help = `Write to a PostgreSQL Database`

  uri = this.params[0]
  options = this.params[1]

  wr: StreamWrapper<NodeJS.WritableStream> | null = null
  db!: pg.Client
  columns!: string[]
  columns_str!: string

  async init() {
    const db = this.db = new pg.Client(`postgres://${await this.uri}`)
    await db.connect()

    if (this.options.notice) {
      db.on('notice', (notice: Error) => {
        const _ = notice as Error & {severity: string}
        this.info(`pg ${_.severity}: ${_.message}`)
      })
    }

    await db.query('BEGIN')
  }

  async end() {
    await this.db.query('COMMIT')
  }

  async final() {
    if (this.wr)
      await this.wr.close()
    await this.db.end()
  }

  async error(err: any) {
    await this.db.query('ROLLBACK')
    throw err
  }

  async onCollectionStart(chunk: Chunk.Data) {
    var payload = chunk.payload
    var table = chunk.collection
    const columns = this.columns = Object.keys(payload)
    var types = columns.map(c => typeof payload[c] === 'number' ? 'real'
    : payload[c] instanceof Buffer ? 'blob'
    : 'text')

    if (this.options.drop) {
      await this.db.query(`DROP TABLE IF EXISTS "${table}"`)
    }

    // Create the table if it didn't exist
    await this.db.query(`
      CREATE TABLE IF NOT EXISTS "${table}" (
        ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
      )
    `)

    // Create a temporary table that will receive all the data through pg COPY
    // command
    await this.db.query(`
      CREATE TEMP TABLE "temp_${table}" (
        ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
      )
    `)

    this.columns_str = columns.map(c => `"${c}"`).join(', ')

    if (this.options.truncate) {
      this.info(`truncating "${table}"`)
      await this.db.query(`DELETE FROM "${table}"`)
    }

    var stream: NodeJS.WritableStream = await this.db.query(copy_from(`COPY temp_${table}(${this.columns_str}) FROM STDIN
    WITH
    DELIMITER AS ';'
    CSV HEADER
    QUOTE AS '"'
    ESCAPE AS '"'
    NULL AS 'NULL'`)) as any

    var csv: NodeJS.ReadWriteStream = _({
      delimiter: ';',
      header: true,
      quote: '"',
      // escape: true
    })

    csv.pipe(stream)
    this.wr = new StreamWrapper(csv)
  }

  async onData(chunk: Chunk.Data) {
    await this.wr!.write(chunk.payload)
  }

  async onCollectionEnd(table: string) {
    if (!this.wr)
      throw new Error('wr is not existing')

    await this.wr.close()
    this.wr = null

    const db_cols = (await this.db.query(`
    select json_object_agg(column_name, udt_name) as res
    from information_schema.columns
    where table_name = '${table}'
    `)).rows[0].res as {[name: string]: string}

    const expr = this.columns.map(c => `"${c}"::${db_cols[c]}`)
    .join(', ')

    var upsert = ""
    if (this.options.upsert) {
      var up = (this.options.upsert as any)[table]
      if (typeof up === 'string') {
        upsert = ` on conflict on constraint "${up}" do update set ${this.columns.map(c => `${c} = EXCLUDED.${c}`)} `
      }
    }

    await this.db.query(`
      INSERT INTO "${table}"(${this.columns_str}) (SELECT ${expr} FROM "temp_${table}")
      ${upsert}
    `)

    await this.db.query(`
      DROP TABLE "temp_${table}"
    `)

  }
}
