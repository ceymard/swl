
import {Sequence, sources, Chunk, ChunkIterator, y, sinks, URI, OPT_OBJECT, StreamWrapper} from 'swl'
import * as pg from 'pg'
import * as _ from 'csv-stringify'
const copy_from = require('pg-copy-streams').from

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


/**
 * COPY ${table}(${heads.join(', ')}) FROM STDIN
				WITH
				DELIMITER AS ';'
				CSV HEADER
				QUOTE AS '"'
				ESCAPE AS '"'
				NULL AS 'NULL'
 */

sinks.add(
`Write to a PostgreSQL Database`,
  y.object({
    truncate: y.boolean().default(false).label('Truncate tables before loading'),
    notice: y.boolean().default(true).label('Show notices on console'),
    drop: y.boolean().default(false).label('Drop tables'),
    upsert: y.object({}).default({}).label('Upsert Column Name')
  }),
  URI,
  function postgres(opts, uri) {
    // const uri = URI.tryParse(rest.trim())
    var wr: StreamWrapper<NodeJS.WritableStream> = null!

    async function* run(db: pg.Client, upstream: ChunkIterator): ChunkIterator {

      var table: string = ''
      var columns: string[] = []
      var start = false
      var columns_str: string = ''

      /**
       * Flush the data into the destination table. This is where
       * we try to know if we're going to upsert or just plain insert.
       */
      async function flush() {
        if (!wr) return

        await wr.close()

        const db_cols = (await db.query(`
        select json_object_agg(column_name, udt_name) as res
        from information_schema.columns
        where table_name = '${table}'
        `)).rows[0].res

        const expr = columns.map(c => `"${c}"::${db_cols[c]}`)
        .join(', ')

        var upsert = ""
        if (opts.upsert) {
          var up = (opts.upsert as any)[table]
          if (typeof up === 'string') {
            upsert = ` on conflict on constraint "${up}" do update set ${columns.map(c => `${c} = EXCLUDED.${c}`)} `
          }
        }

        await db.query(`
          INSERT INTO "${table}"(${columns_str}) (SELECT ${expr} FROM "temp_${table}")
          ${upsert}
        `)

        await db.query(`
          DROP TABLE "temp_${table}"
        `)
      }

      for await (var ev of upstream) {
        if (ev.type === 'start') {
          await flush()
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

            await db.query(`
              CREATE TEMP TABLE "temp_${table}" (
                ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
              )
            `)

            columns_str = columns.map(c => `"${c}"`).join(', ')

            if (opts.truncate) {
              await db.query(`DELETE FROM "${table}"`)
            }

            var stream: NodeJS.WritableStream = await db.query(copy_from(`COPY temp_${table}(${columns_str}) FROM STDIN
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
            wr = new StreamWrapper(csv)

            start = false
          }

          await wr.write(payload)

        } else yield ev
      }

      await flush()

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
        // We have to close wr to make sure we can pass the next
        // query, as it will have them freeze otherwise.
        if (wr)
          await wr.close()

        await db.query('ROLLBACK')
        throw e
      } finally {
        await db.end()
      }
    }
  }, 'postgres', 'pg'
)
