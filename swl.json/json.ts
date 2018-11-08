
import {Chunk, Source, register, s} from 'swl'


@register('inline')
export class InlineJson extends Source(s.string()) {
  help = `Inline Json`
  options_parser = null
  body_parser = null

  async emit() {
    var rest = this.params.trim()
    if (rest[0] !== '[')
      rest = `[${rest}]`
    for (var c of eval(rest)) {
      await this.send(Chunk.data('json', c))
    }
  }
}


/*
sources.add(
`Read json files`,
  y.object({}),
  URI_WITH_OPTS,
  function json (opts, [uri, options]) {
    const sources = make_read_creator(uri, options)

    async function *handleSource(sw: StreamWrapper<any>): ChunkIterator {
      var in_string = false
      var in_obj = false
      var chunk
      var tmp = ''
      var pos = 0
      var count = 0
      var obj_start = 0
      // yield Chunk.start(this.collection)
      while ( (chunk = await sw.read()) !== null) {
        const st: string = chunk instanceof Buffer ? chunk.toString('utf-8') : chunk as string
        tmp += st

        for (; pos < tmp.length; pos++) {
          if (!in_string) {
            if (tmp[pos] === '{') {
              in_obj = true
              if (count === 0)
                obj_start = pos
              count += 1
            } else if (tmp[pos] === '}') {
              count -= 1
              in_obj = false
              if (count === 0) {
                var obj = tmp.slice(obj_start, pos + 1)
                yield Chunk.data(JSON.parse(obj))
                // We remove the excess object and start again
                tmp = tmp.slice(pos + 1)
                pos = -1
              }
            } else if (in_obj && tmp[pos] === '"') {
              in_string = true
            }
          } else {
            if (in_obj && tmp[pos] === '"' && tmp[pos - 1] !== '\\') {
              in_string = false
            }
          }
        }

      }

    }

    return async function *json(upstream: ChunkIterator): ChunkIterator {
      yield* upstream

      for await (var s of sources) {
        yield Chunk.start(s.collection)
        yield* handleSource(new StreamWrapper(s.source))
      }
    }

  },
  'json', '.json'
)


sinks.add(
`Write a JSON file`,
  y.object({}),
  URI_WITH_OPTS,
  function json(opts, [uri, options]) {

    return async function *json(upstream): ChunkIterator {

      var creator = await make_write_creator(uri, options)
      var wr!: StreamWrapper<NodeJS.WritableStream>

      for await (var chk of upstream) {

        if (chk.type === 'start') {
          wr = await creator(chk.name)
        } else if (chk.type === 'data') {
          await wr.write(JSON.stringify(chk.payload) + '\n')
        } else yield chk
      }

    }

  },
  'json', '.json'
)
*/