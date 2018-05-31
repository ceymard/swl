
import {Sequence, sources, Chunk, ChunkIterator, y, sinks, URI, OPT_OBJECT} from 'swl'
import * as pg from 'pg'


sources.add(
`Read from a PostgreSQL database`,
  y.object(),
  Sequence(URI, OPT_OBJECT),
  async function postgres(options, [uri, src]) {
    var sources = src || {}

  return async function *(upstream: ChunkIterator): ChunkIterator {
    yield* upstream

    uri = `postgres://${uri}`
    const db = new pg.Client(uri)
    await db.connect()

    var keys = Object.keys(sources)
    if (keys.length === 0) {
      const tables = await db.query(`
        SELECT * FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'`)

      for (let res of tables.rows) {
        sources[res.table_name] = true
      }
      keys = Object.keys(sources)
    }
    // console.log(sources)

    for (var colname of keys) {
      var val = sources[colname]

      var sql = typeof val !== 'string' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      const result = await db.query(sql)

      yield Chunk.start(colname)
      for (var s of result.rows) {
        yield Chunk.data(s)
      }
    }

    await db.end()
  }
}, 'postgres', 'pg')


sinks.add(
`Write to a PostgreSQL Database`,
  y.object({
    truncate: y.boolean().default(false).label('Truncate tables before loading'),
    notice: y.boolean().default(true).label('Show notices on console'),
    drop: y.boolean().default(false).label('Drop tables'),
  }),
  URI,
  function postgres(opts, uri) {
    // const uri = URI.tryParse(rest.trim())
    const mode: 'insert' | 'upsert' | 'update' = 'insert'

    async function* run(db: pg.Client, upstream: ChunkIterator): ChunkIterator {

      var table: string = ''
      var columns: string[] = []
      var start = false
      var text = ''
      var query_name = ''

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
              await db.query(`DROP TABLE IF EXISTS "${table}"`)
            }

            // Create if not exists ?
            // Temporary ?
            await db.query(`
              CREATE TABLE IF NOT EXISTS "${table}" (
                ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
              )
            `)

            if (mode === 'insert') {
              query_name = `swl-query-insert-${table}`
              text = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
              values (${columns.map((c, i) => `$${i + 1}`).join(', ')})`
              // console.log(sql)
            }
            else if (mode === 'upsert') {
              query_name = `swl-query-insert-${table}`
              // Should I do some sub-query thing with coalesce ?
              // I would need some kind of primary key...
              text = `INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
                values (${columns.map((c, i) => `$${i + 1}`).join(', ')})`
            }


            if (opts.truncate) {
              await db.query(`DELETE FROM "${table}"`)
            }
            start = false
          }

          await db.query({
            name: query_name,
            text,
            values: columns.map(c => payload[c])
          })

        } else yield ev
      }

    }

    return async function *postgres(upstream: ChunkIterator): ChunkIterator {

      const db = new pg.Client(`postgres://${await uri}`)
      await db.connect()

      if (opts.notice) {
        db.on('notice', (notice: Error) => {
          const _ = notice as Error & {severity: string}
          console.log(`pg ${_.severity}: ${_.message}`)
        })
      }

      try {
        await db.query('BEGIN')
        yield* run(db, upstream)
        await db.query('COMMIT')
      } catch (e) {
        await db.query('ROLLBACK')
        throw e
      } finally {
        await db.end()
      }
    }
  }, 'postgres', 'pg'
)
