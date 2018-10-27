
import { s, Source, Sequence, URI, OPT_OBJECT, ParserType, Chunk, register, Lock } from 'swl'

import * as m from 'mssql'


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

    var l = new Lock
    var db = this.db = new m.ConnectionPool(uri, err => {
      if (err)
        return l.reject(err)
      l.resolve()
    })
    await l.promise

    var keys = Object.keys(sources)
    if (keys.length === 0) {
      // Get the list of all the tables if we did not know them.
      await this.send(Chunk.info(this, 'Getting table list'))
      const tables = (await db.request().query(`
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