
import { s, Source, Sequence, URI, OPT_OBJECT, ParserType, Chunk, register, Sink } from 'swl'

import * as m from 'mssql'

declare module 'mssql' {
  interface ConnectionPool {
    query(s: string): Promise<m.IResult<any>>
  }
}


const MSSQL_SRC_OPTIONS = s.object({

})
const MSSQL_SRC_BODY = Sequence(URI, OPT_OBJECT)


@register('mssql', 'ms')
export class MssqlSource extends Source<
  s.BaseType<typeof MSSQL_SRC_OPTIONS>,
  ParserType<typeof MSSQL_SRC_BODY>
> {

  help = ``
  options_parser = MSSQL_SRC_OPTIONS
  body_parser = MSSQL_SRC_BODY

  sources: {[name: string]: boolean | string} = {}
  db!: m.ConnectionPool

  async emit() {
    var [_uri, sources] = this.body
    var uri = `mssql://${await _uri}`

    if (sources)
      this.sources = sources

    var db = this.db = new m.ConnectionPool(uri)
    await db.connect()

    var keys = Object.keys(sources)
    if (keys.length === 0) {
      // Get the list of all the tables if we did not know them.
      await this.send(Chunk.info(this, 'Getting table list'))
      const tables = (await db.query(`
        SELECT table_name FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' and table_name not like 'sys%'`)).recordset

      for (let res of tables) {
        if (res === 'sysdiagrams') continue
        sources[res.table_name] = true
      }
      keys = Object.keys(sources)
    }

    for (var colname of keys) {
      var val = sources[colname]
      await this.send(Chunk.info(this, `Processing ${colname}`))

      var sql = typeof val !== 'string' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      const result = await db.request().query(sql)

      for (var s of result.recordset) {
        await this.send(Chunk.data(colname, s))
      }
    }

    await db.close()

  }

}



const MSSQL_SINK_OPTIONS = s.object({
  truncate: s.boolean(false).help`Truncate tables before loading`,
  notice: s.boolean(true).help`Show notices on console`,
  drop: s.boolean(false).help`Drop tables`,
  merge: s.object({}).help`Upsert Column Name`
})


@register('mssql', 'ms')
export class MssqlSink extends Sink<
  s.BaseType<typeof MSSQL_SINK_OPTIONS>,
  ParserType<typeof URI>
> {
  help = `Write to a PostgreSQL Database`
  options_parser = MSSQL_SINK_OPTIONS
  body_parser = URI

  db!: m.ConnectionPool
  transaction!: m.Transaction
  statement!: m.PreparedStatement
  columns!: string[]
  columns_str!: string
  temp!: string
  table!: string

  async init() {
    const db = this.db = new m.ConnectionPool(`mssql://${await this.body}`)
    await db.connect()
    var tra = this.transaction = await db.transaction()
    await tra.begin()
  }

  async query(req: TemplateStringsArray, ...args: any[]): Promise<m.IResult<any>>
  async query(req: string): Promise<m.IResult<any>>
  async query(req: TemplateStringsArray | string, ...args: any[]) {
    var request = this.transaction.request()
    if (typeof req === 'string') {
      return await request.query(req)
    }

    var r = [] as string[]
    for (var i = 0; i < req.length; i++) {
      r.push(req[i])
      if (args && i < args.length) {
        var name = `par${i}`
        request.input(name, args[i])
        r.push(`@${name}`)
      }
    }

    return await request.query(r.join(' '))
  }

  async end() {
    await this.transaction.commit()
  }

  async final() {
    await this.db.close()
  }

  async error(err: any) {
    // console.log(err.stack)
    if (this.statement)
      await this.statement.unprepare()
    await this.transaction.rollback()
  }

  async onCollectionStart(chunk: Chunk.Data) {
    var payload = chunk.payload
    var table = this.table = chunk.collection
    const columns = this.columns = Object.keys(payload)
    var types = columns.map(c => typeof payload[c] === 'number' ? 'real'
    : payload[c] instanceof Date ? 'datetime'
    : payload[c] instanceof Buffer ? 'binary'
    : 'nvarchar(max)')

    if (this.options.drop) {
      await this.query(`DROP TABLE IF EXISTS [${table}]`)
    }

    // Create the table if it didn't exist
    await this.query(`
      IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='${table}' AND xtype='U')

      CREATE TABLE [${table}] (
        ${columns.map((c, i) => `[${c}] ${types[i]}`).join(', ')}
      )
    `)

    // Create a temporary table that will receive all the data through pg COPY
    // command
    await this.query(`
      CREATE TABLE ##temp_table (
        ${columns.map((c, i) => `[${c}] ${types[i]}`).join(', ')}
      )
    `)

    this.columns_str = columns.map(c => `[${c}]`).join(', ')

    if (this.options.truncate) {
      await this.query(`DELETE FROM [${table}]`)
    }

    this.statement = new m.PreparedStatement(this.transaction as any)

    for (var c of this.columns) {
      var type = typeof payload[c] === 'number' ? m.Real
      : payload[c] instanceof Date ? m.DateTime
      : payload[c] instanceof Buffer ? m.Binary
      : m.NVarChar
      this.statement.input(c, type)
    }
    var stmt = `INSERT INTO [##temp_table](${this.columns_str}) VALUES (${this.columns.map(c => `@${c}`).join(', ')})`
    await this.statement.prepare(stmt)
  }

  async onData(chunk: Chunk.Data) {
    await this.statement.execute(chunk.payload)
  }

  async onCollectionEnd(table: string) {
    await this.statement.unprepare()
    this.statement = null!

    // var upsert = ""
    if (this.options.merge) {
      var res: m.IResult<{table: string, column: string}> = await this.query`
        SELECT KU.table_name as [table], column_name as [column]
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        INNER JOIN
          INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                   TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
                   KU.table_name = ${this.table}
        ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;`

      if (!res.recordset[0]) {
        throw new Error(`table [${this.table}] does not have a primary key`)
      }

      var col = res.recordset[0].column as string

      var all_columns = this.columns
      // Get all the columns that we're inserting that are NOT the primary key
      var columns = this.columns.filter(c => c!== col)

      var has_identity = (await this.query`SELECT name FROM sys.identity_columns
        WHERE OBJECT_NAME(OBJECT_ID) = ${this.table}`).recordset.length > 0

      // We should probably compute what is the primary key of the table instead of assuming
      // the column is called id
      var res2 = await this.query(`
        ${has_identity ? `SET IDENTITY_INSERT [${this.table}] ON;` : ''}

        MERGE INTO [${this.table}] as TARGET
        USING (
          SELECT * FROM [##temp_table]
        ) as SOURCE
        ON (SOURCE.[${col}] = TARGET.[${col}])
        WHEN NOT MATCHED THEN
          INSERT (${all_columns.map(c => `[${c}]`).join(', ')})
          VALUES (${all_columns.map(c => `SOURCE.[${c}]`).join(', ')})
        WHEN MATCHED THEN
          UPDATE SET ${columns.map(c => `TARGET.[${c}] = SOURCE.[${c}]`).join(', ')}
        ;

        ${has_identity ? `SET IDENTITY_INSERT [${this.table}] OFF;` : ''}
        DROP TABLE ##temp_table;
      `)

      await this.send(Chunk.info(this, res2.rowsAffected.join(", ") + ' rows affected for ' + this.table))
    } else {
      await this.query(`
        INSERT INTO [${table}](${this.columns_str})
          SELECT ${this.columns_str} FROM [##temp_table];

        DROP TABLE ##temp_table;
      `)
    }

  }
}
