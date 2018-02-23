import {Source, PipelineEvent, register_source, StreamSource, make_read_creator, StreamSink, PipelineComponent, WriteStreamCreator, resume_once, register_sink, make_write_creator, URI_WITH_OPTS} from 'swl'

var id = 0
export class InlineJson extends Source {

  objects: any[]

  constructor(public options: any, public body: string) {
    super(options)
    this.objects = JSON.parse(`[${body}]`)
  }

  async *emit(): AsyncIterableIterator<PipelineEvent> {
    yield this.start(Object.keys(this.options)[0] || `json-${id++}`)
    for (var o of this.objects) {
      yield this.data(o)
    }
  }

}


export class JsonSource extends StreamSource {

  async *handleSource(): AsyncIterableIterator<PipelineEvent> {
    var in_string = false
    var in_obj = false
    var chunk
    var tmp = ''
    var pos = 0
    var count = 0
    var obj_start = 0
    yield this.start(this.collection)
    while ( (chunk = await this.read()) !== null) {
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
              yield this.data(JSON.parse(obj))
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

}


export class JsonSink extends PipelineComponent {

  constructor(options: any, public creator: WriteStreamCreator) {
    super(options)
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {
    var stream: NodeJS.WritableStream | undefined
    var writable: NodeJS.WritableStream | undefined
    var start = true

    function close() {
      if (writable) writable.end()
      if (stream) stream.end()
    }

    async function write(str: string) {
      if (!writable!.write(str))
      await resume_once(writable!, 'drain')
    }

    for await (var ev of this.upstream()) {
      if (ev.type === 'start') {
        close()

        var new_writable = await this.creator(ev.name)
        var diff = new_writable !== writable
        writable = new_writable
        if (diff) {
          if (this.options.object) {
            if (start = true)
              await write(`{`)
            await write(`"${ev.name}":[`)
          } else {

          }
        }
      } else if (ev.type === 'data') {
        await write(JSON.stringify(ev.payload))
      } else {
        // Other events should still go down the drain
        yield ev
      }
    }

    close()
  }

}

register_source(async (opts: any, str: string) => {
  return new InlineJson(opts, str)
}, 'inline-json')


register_source(async (opts:any, str: string) => {
  return new JsonSource(opts, await make_read_creator(str.trim(), {}))
},
  'json',
  '.json',
  'text/json',
  'application/json',
  'application/javascript',
  'application/ld+json',
  'application/vnd.geo+json'
)

register_sink(async (opts: any, str: string) => {
  const [uri, options] = URI_WITH_OPTS.tryParse(str)
  return new JsonSink(opts, await make_write_creator(uri, options))
}, 'json', '.json')