
import { s, Source, Chunk, register, Sink, open_tunnel } from 'swl'

import * as m from 'mssql'

declare module 'mssql' {
  interface ConnectionPool {
    query(s: string): Promise<m.IResult<any>>
  }
}


const MSSQL_SRC = s.tuple(
  s.string().then(s => s.ok(open_tunnel(s.value))), // the URI
  s.object(), // the options
  s.array(s.indexed( // at last, the sources
    s.boolean()
    .or(s.string())
  ))
)


@register('mssql', 'ms')
export class MssqlSource extends Source(MSSQL_SRC) {

  help = ``

  uri = this.params[0]
  options = this.params[1]
  sources = this.params[2]
  db!: m.ConnectionPool

  async emit() {
    var uri = `mssql://${await this.uri}`

    var db = this.db = new m.ConnectionPool(uri)
    await db.connect()

    var sources = [] as {name: string, query: string}[]

    if (this.sources.length === 0) {
      // Get the list of all the tables if we did not know them.
      this.info('Getting table list')
      const tables = (await db.query(`
        SELECT table_name FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' and table_name not like 'sys%'`)).recordset

      for (let res of tables) {
        if (res === 'sysdiagrams') continue
        sources.push({name: res.table_name, query: `select * FROM "${res.table_name}"`})
      }
    } else {
      for (var src of this.sources) {
        for (var x in src) {
          var spec = src[x]
          sources.push({
            name: x,
            query: typeof spec === 'string' ? spec : `select * from "${x}"`
          })
        }
      }
    }

    for (var source of sources) {
      var name = source.name
      this.info(`Processing ${name}`)

      const result = await db.request().query(source.query)

      for (var s of result.recordset) {
        await this.send(Chunk.data(name, s))
      }
    }

    await db.close()

  }

}


/**
 * A Table handler since there is a lot of work to do with MSSQL.
 */
export class MssqlTableHandler {

  public primary_key: string = ''
  public table_has_identity: boolean = false
  public columns_sans_id = [] as string[]
  public table_obj!: m.Table
  public columns_str!: string

  constructor(
    public sink: MssqlSink,
    public table_name: string,
    public columns: string[]
  ) {
    this.columns_str = columns.map(c => `[${c}]`).join(', ')
  }

  async init() {
    await this.handlePrimaryKey()
    await this.initTableObj()
  }

  /**
   * Get the primary key from the table and figure out if
   * the table has an identity column
   */
  async handlePrimaryKey() {
    var res: m.IResult<{table: string, column: string}> = await this.sink.query`
    SELECT KU.table_name as [table], column_name as [column]
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
    INNER JOIN
      INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
            ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
               TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
               KU.table_name = ${this.table_name}
    ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;`

    if (!res.recordset[0]) {
      throw new Error(`table [${this.table_name}] does not have a primary key`)
    }

    var col = res.recordset[0].column as string
    this.primary_key = col

    // Get all the columns that we're inserting that are NOT the primary key
    this.columns_sans_id = this.columns.filter(c => c!== col)

    this.table_has_identity = (await this.sink.query`SELECT name FROM sys.identity_columns
      WHERE OBJECT_NAME(OBJECT_ID) = ${this.table_name}`).recordset.length > 0
  }

  async initTableObj() {
    var res = await this.sink.query(`SELECT top 0 ${this.columns.map(c => `[${c}]`).join(', ')} FROM [${this.table_name}]`)
    var res_cols = res.recordset.columns

    this.table_obj = new m.Table('#temp_tbl')

    for (var c of this.columns) {
      var rc = res_cols[c]
      this.table_obj.columns.add(rc.name, rc, {
        nullable: (rc as any).nullable
      })
    }

    this.table_obj.create = true
  }

  async row(payload: any) {
    var c = this.columns
    var r = new Array(c.length)
    for (var i = 0; i < c.length; i++) {
      r[i] = payload[c[i]]
    }
    this.table_obj.rows.push(r)

    if (this.table_obj.rows.length > 1024)
      await this.flush()
  }

  /**
   * Flush the rows we already have in the table, and create
   */
  async flush() {
    var r = this.sink.transaction.request()
    await r.bulk(this.table_obj)

    // Reset the rows, we do not want to use the add() method so a plain array
    // should be just fine.
    this.table_obj.rows = [] as unknown as m.rows
  }

  async end() {
    await this.sink.query(`DROP TABLE #temp_tbl;`)
  }
}


const MSSQL_SINK_OPTIONS = s.tuple(
  s.string().then(r => open_tunnel(r.value)),
  s.object({
    truncate: s.boolean(false).help`Truncate tables before loading`,
    notice: s.boolean(true).help`Show notices on console`,
    drop: s.boolean(false).help`Drop tables`,
    merge: s.object({}).help`Upsert Column Name`
  })
)


@register('mssql', 'ms')
export class MssqlSink extends Sink(MSSQL_SINK_OPTIONS) {
  help = `Write to a PostgreSQL Database`

  uri = this.params[0]
  options = this.params[1]

  db!: m.ConnectionPool
  transaction!: m.Transaction
  tbl!: MssqlTableHandler

  values: any[][] = []
  stmt_nb = 0

  async init() {
    const db = this.db = new m.ConnectionPool(`mssql://${await this.uri}`)
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
    await this.transaction.rollback()
  }

  async onCollectionStart(chunk: Chunk.Data) {
    var payload = chunk.payload

    var table = chunk.collection
    const columns = Object.keys(payload)

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

    if (this.options.truncate) {
      await this.query(`DELETE FROM [${table}]`)
    }

    this.tbl = new MssqlTableHandler(
      this,
      chunk.collection,
      Object.keys(chunk.payload)
    )
    await this.tbl.init()
  }

  async onData(chunk: Chunk.Data) {
    await this.tbl.row(chunk.payload)
  }

  async onCollectionEnd(table: string) {
    var tbl = this.tbl
    await tbl.flush()

    var PK = tbl.primary_key
    var should_id = tbl.table_has_identity ? `SET IDENTITY_INSERT [${tbl.table_name}] ON;` : ''

    var sql = `
    ${should_id}
    INSERT INTO [${tbl.table_name}](${tbl.columns_str})
    SELECT ${tbl.columns.map(c => `SOURCE.[${c}]`)} FROM #temp_tbl SOURCE
    LEFT JOIN ${tbl.table_name} TARGET ON TARGET.[${PK}] = SOURCE.[${PK}]
    WHERE TARGET.[${PK}] IS NULL
    `
    await this.query(sql)

    if (this.options.merge) {
      // We're going to update too !
      var sql = `
        ${should_id}
        UPDATE [${tbl.table_name}]
        SET ${tbl.columns_sans_id.map(c => `[${c}] = SOURCE.[${c}]`).join(', ')}
        FROM (
          SELECT ${tbl.columns_str}
          FROM #temp_tbl
        ) AS SOURCE
        WHERE SOURCE.[${PK}] = [${tbl.table_name}].[${PK}]
      `
      // console.log(sql)
      await this.query(sql)

      // this.info(res2.rowsAffected.join(", ") + ' rows affected for ' + this.table)
    }

    if (tbl.table_has_identity) {
      await this.query(`SET IDENTITY_INSERT [${tbl.table_name}] OFF`)
    }

    await tbl.end()
  }
}
