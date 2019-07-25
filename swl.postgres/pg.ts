
import {Sequence, Chunk, s, Sink, URI, OPT_OBJECT, StreamWrapper, Source, ParserType, register} from 'swl'
import * as pg from 'pg'
import * as _ from 'csv-stringify'
const copy_from = require('pg-copy-streams').from


var types = pg.types
// Data type !
types.setTypeParser(1082, val => {
  // var d = new Date(val)
  return val
})

const PG_SRC_OPTIONS = s.object({
  schema: s.string('public')
})
const PG_SRC_BODY = Sequence(URI, OPT_OBJECT)

@register('pg', 'postgres')
export class PgSource extends Source<
  s.BaseType<typeof PG_SRC_OPTIONS>,
  ParserType<typeof PG_SRC_BODY>
> {
  help = `Read from a PostgreSQL database`
  body_parser = PG_SRC_BODY
  options_parser = PG_SRC_OPTIONS

  sources: {[name: string]: boolean | string} = {}
  db!: pg.Client

  async emit() {
    var [_uri, sources] = this.body
    var uri = `postgres://${await _uri}`

    if (sources)
      this.sources = sources

    const db = new pg.Client(uri)
    await db.connect()
    this.db = db

    var keys = Object.keys(sources)
    if (keys.length === 0) {
      const tbls = await db.query(`
      WITH cons AS (SELECT
        tc.table_schema,
        tc.constraint_name,
        tc.table_name,
        kcu.column_name,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM
          information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'),
          tbls AS (SELECT tbl.table_schema as "schema", tbl.table_schema || '.' || tbl.table_name as tbl, cons.foreign_table_schema || '.' || cons.foreign_table_name as dep FROM
          "information_schema"."tables" tbl
          LEFT JOIN cons ON cons.table_name = tbl.table_name AND cons.table_schema = tbl.table_schema
      where tbl.table_schema = '${this.options.schema}' AND tbl.table_type = 'BASE TABLE')
      SELECT t.tbl, COALESCE(array_agg(t.dep) FILTER (WHERE t.dep IS NOT NULL), '{}'::text[]) as deps
      FROM tbls t
      GROUP BY t.tbl
      `)

      const dct = {} as {[name: string]: string[]}
      for (let r of tbls.rows) {
        dct[r.tbl] = r.deps
      }

      const tables_set = new Set<string>()
      const add_deps = (tbl: string) => {
        for (var t of dct[tbl]) {
          if (!tables_set.has(t) && t !== tbl)
            add_deps(t)
        }
        tables_set.add(tbl)
      }

      for (var tblname in dct) {
        add_deps(tblname)
      }
      keys = Array.from(tables_set)
    }

    for (var colname of keys) {
      var val = sources[colname]

      var sql = typeof val !== 'string' ? `SELECT * FROM "${colname.replace('.', '"."')}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val.replace('.', '"."')}"`
      : val

      const result = await db.query(sql)

      for (var s of result.rows) {
        // console.log(s)
        await this.send(Chunk.data(colname, s))
      }
    }

  }

  async end() {
    await this.db.end()
  }

}



const PG_SINK_OPTIONS = s.object({
  truncate: s.boolean(false).default(false).help`Truncate tables before loading`,
  notice: s.boolean(true).default(true).help`Show notices on console`,
  drop: s.boolean(false).default(false).help`Drop tables`,
  upsert: s.object({}).help`Upsert Column Name`,
  disable_triggers: s.boolean(false)
})


@register('pg', 'postgres')
export class PgSink extends Sink<
  s.BaseType<typeof PG_SINK_OPTIONS>,
  ParserType<typeof URI>
> {
  help = `Write to a PostgreSQL Database`
  options_parser = PG_SINK_OPTIONS
  body_parser = URI

  wr: StreamWrapper<NodeJS.WritableStream> | null = null
  db!: pg.Client
  columns!: string[]
  columns_str!: string

  async init() {
    const db = new pg.Client(`postgres://${await this.body}`)
    await db.connect()
    this.db = db

    if (this.options.notice) {
      db.on('notice', (notice: Error) => {
        const _ = notice as Error & {severity: string}
        this.info(`pg ${_.severity}: ${_.message}`)
      })
    }

    if (this.options.disable_triggers)
      await db.query(`SET session_replication_role = replica;`)

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
    try {
      if (this.db) await this.db.query('ROLLBACK')
    } finally {
      throw err
    }
  }

  async onCollectionStart(chunk: Chunk.Data) {
    var payload = chunk.payload
    var table = chunk.collection
    const columns = this.columns = Object.keys(payload)
    var types = columns.map(c => typeof payload[c] === 'number' ? 'real'
    : payload[c] instanceof Date ? 'timestamptz'
    : payload[c] instanceof Buffer ? 'blob'
    : 'text')
    // console.log(chunk.collection, types)

    if (this.options.drop) {
      await this.db.query(`DROP TABLE IF EXISTS ${table}`)
    }

    // Create the table if it didn't exist
    await this.db.query(`
      CREATE TABLE IF NOT EXISTS ${table} (
        ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
      )
    `)

    // Create a temporary table that will receive all the data through pg COPY
    // command
    await this.db.query(`
      CREATE TEMP TABLE ${table.replace('.', '_')}_temp (
        ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
      )
    `)

    this.columns_str = columns.map(c => `"${c}"`).join(', ')

    if (this.options.truncate) {
      this.info(`truncating ${table}`)
      await this.db.query(`DELETE FROM ${table}`)
    }

    var stream: NodeJS.WritableStream = await this.db.query(copy_from(`COPY ${table.replace('.', '_')}_temp(${this.columns_str}) FROM STDIN
    WITH
    DELIMITER AS ';'
    CSV HEADER
    QUOTE AS '"'
    ESCAPE AS '"'
    NULL AS '**NULL**'`)) as any

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
    var data = {} as any
    var p = chunk.payload
    for (var x in p) {
      const val = p[x]
      if (val === null)
        data[x] = '**NULL**'
      else if (val instanceof Date)
        data[x] = val!.toUTCString()
      else if (val instanceof Array)
        data[x] = '{' + val.join(',') + '}'
      else if (val.constructor === Object)
        data[x] = JSON.stringify(val)
      else
        data[x] = val.toString()
    }
    await this.wr!.write(data)
  }

  async onCollectionEnd(table: string) {
    if (!this.wr)
      throw new Error('wr is not existing')

    this.info(`closing pipe`)
    await this.wr.close()
    this.wr = null

    var schema = 'public'
    var tbl = table
    if (table.includes('.')) {
      [schema, tbl] = table.split('.')
    }

    const db_cols = (await this.db.query(`
    select json_object_agg(column_name, udt_name) as res
    from information_schema.columns
    where table_name = '${tbl}' AND table_schema = '${schema}'
    `)).rows[0].res as {[name: string]: string}

    const expr = this.columns.map(c => `"${c}"::${db_cols[c]}`)
    .join(', ') // .replace(/timestamp(tz)?/g, 'long::abstime')
    // this.info(expr)

    var upsert = ""
    if (this.options.upsert) {
      var schema = 'public'
      var tbl = table
      if (table.includes('.')) {
        [schema, tbl] = table.split('.')
      }
      var cst = (await this.db.query(`SELECT constraint_name, table_name, column_name, ordinal_position
      FROM information_schema.key_column_usage
      WHERE table_name = '${tbl}' AND constraint_schema = '${schema}' AND constraint_name LIKE '%_pkey';`))

      upsert = ` ON CONFLICT ON CONSTRAINT "${cst.rows[0].constraint_name}" DO UPDATE SET ${this.columns.map(c => `"${c}" = EXCLUDED."${c}"`)} `
    }

    this.info(`inserting data from ${table.replace('.', '_')}_temp`)

    // Insert data from temp table into final table
    // console.log((await this.db.query(`SELECT * FROM ${table.replace('.', '_')}_temp`)).rows)

    await this.db.query(`
      INSERT INTO ${table}(${this.columns_str}) (SELECT ${expr} FROM ${table.replace('.', '_')}_temp)
      ${upsert}
    `)

    // Drop the temporary table
    await this.db.query(`
      DROP TABLE ${table.replace('.', '_')}_temp
    `)

    // Reset sequences if needed
    const seq_res = await this.db.query(/* sql */`
      SELECT
        column_name as name,
        regexp_replace(
          regexp_replace(column_default, '[^'']+''', ''),
          '''.*',
          ''
        ) as seq
      FROM information_schema.columns
      WHERE table_name = '${tbl}'
        AND table_schema = '${schema}'
        AND column_default like '%nextval(%'
    `)

    const sequences = seq_res.rows as {name: string, seq: string}[]

    for (var seq of sequences) {
      this.info(`Resetting sequence ${seq.seq}`)
      await this.db.query(/* sql */`
        DO $$
        DECLARE
          themax INT;
        BEGIN
          LOCK TABLE ${table} IN EXCLUSIVE MODE;
          SELECT MAX(${seq.name}) INTO themax FROM ${table};
          PERFORM SETVAL('${seq.seq}', COALESCE(themax + 1, 1), false);
        END
        $$ LANGUAGE plpgsql;
      `)
    }

  }
}
