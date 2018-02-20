
import {Source, PipelineEvent} from 'swl'
import * as S from 'better-sqlite3'


export type Selector = boolean | string


export class SqliteSource extends Source {

  constructor(public filename: string, public options = {}, public sources: {[name: string]: Selector} = {}) {
    super()
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    const db = new S(this.filename, this.options)

    for (var colname in this.sources) {
      var val = this.sources[colname]

      var sql = typeof val === 'boolean' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      var stmt = db.prepare(sql)

      yield this.event('start', colname)
      for (var s of stmt.iterate()) {
        yield this.event('data', s)
      }
    }
  }

}