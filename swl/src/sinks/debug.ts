
import { ChunkIterator, Transformer, register, Sink, Chunk } from '../pipeline'
import * as s from '../slz'

import ch from 'chalk'

const c = ch.constructor({level: 3})
const prop = c.hsl(180, 30, 30)
const str = c.hsl(80, 60, 60)
const num = c.hsl(120, 60, 60)
const constant = c.hsl(0, 60, 60)
const bool = c.hsl(280, 60, 60)
const coll = c.hsl(220, 60, 60)
const info = c.hsl(40, 60, 60)

export function print_value(out: NodeJS.WritableStream, obj: any, outside = true) {

  if (obj == null) {
    out.write(constant(obj))
  } else if (typeof obj === 'string') {
    out.write(str(obj.replace('\n', '\\n') || "''"))
  } else if (typeof obj === 'number') {
    out.write(num(obj as any))
  } else if (typeof obj === 'boolean') {
    out.write(bool(obj as any))
  } else if (typeof obj === 'object') {
    if (!outside)
      out.write('{')
    var first = true
    for (var x in obj) {
      if (!first) out.write(prop(', '))
      out.write(prop(x + ': '))
      print_value(out, obj[x], false)
      first = false
    }
    if (!outside)
      out.write('}')
  } else {
    out.write(obj)
  }
}


const DEBUG_OPTIONS = s.object({
  data: s.boolean().default(true),
  other: s.boolean().default(true)
})


@register('debug')
export class DebugTransformer extends Transformer<{data: boolean, other: boolean}, []> {

  help = `Print chunks to the console.

  Note that a debug sink is always appended by default
  -- if it prints nothing it is because another
  sink handled the chunks without passing them along.`

  options_parser = DEBUG_OPTIONS
  body_parser = null

  current_collection: string = ''
  nb = 0

  async *onCollectionStart(chunk: Chunk.Start): ChunkIterator {
    this.current_collection = chunk.name
    this.nb = 0
    yield chunk
  }

  async *onData(chunk: Chunk.Data): ChunkIterator {

    if (this.options.data) {
      this.nb++
      process.stdout.write(coll(`${this.current_collection}: ${this.nb} `))
      print_value(process.stdout, chunk.payload)
      process.stdout.write('\n')
    }

    yield chunk
  }

  async *onInfo(chunk: Chunk.Info): ChunkIterator {
    process.stdout.write(info(`${chunk.source}: `) + chunk.message + '\n')
    yield chunk
  }
}



@register('null')
export class NullSink extends Sink {

  options_parser = null
  body_parser = null
  help = `Lose the chunks and don't forward them.`

}
