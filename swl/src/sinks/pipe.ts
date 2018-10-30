import { Sink, register, Chunk, Source } from "../pipeline";
import { StreamWrapper } from "../streams";


@register('pipe')
export class PipeSource extends Source<{}, []> {

  help = `A piping to use with unix pipes`
  options_parser = null
  body_parser = null

  async emit() {
    const sw = new StreamWrapper(process.stdin)
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
              await this.send(JSON.parse(obj, (_, value) => {
                if (typeof value === 'string') {
                  var a = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)Z$/.exec(value);
                  if (a) {
                    return new Date(Date.UTC(+a[1], +a[2] - 1, +a[3], +a[4], +a[5], +a[6]));
                  }
                }
                return value
              }) as Chunk)
              // yield Chunk.data(JSON.parse(obj))
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


@register('pipe')
export class PipeSink extends Sink<
  {}, []
> {

  help = `A piping to use with unix pipes`
  options_parser = null
  body_parser = null

  async onData(chunk: Chunk.Data) {
    console.log(JSON.stringify(chunk))
  }

}