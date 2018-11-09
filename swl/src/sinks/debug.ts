
import { Transformer, register, Sink, Chunk } from '../pipeline'
import * as s from 'slz'

import ch from 'chalk'

const c = ch.constructor({level: 3})
export const constant = c.hsl(0, 60, 60)
export const info = c.hsl(40, 60, 60)
export const str = c.hsl(80, 60, 60)
export const num = c.hsl(120, 60, 60)
export const date = c.hsl(140, 60, 60)
export const prop = c.hsl(180, 30, 30)
export const coll = c.hsl(220, 60, 60)
export const bool = c.hsl(280, 60, 60)



const fmt = Intl.DateTimeFormat('default', {
  hour12: false,
  year: 'numeric',
  month: '2-digit',
  day: 'numeric',
  hour: 'numeric',
  minute: 'numeric',
  second: 'numeric'
})

export function print_value(out: NodeJS.WritableStream, obj: any, outside = true) {

  if (obj == null) {
    out.write(constant(obj))
  } else if (typeof obj === 'string') {
    out.write(str(obj.replace('\n', '\\n') || "''"))
  } else if (typeof obj === 'number') {
    out.write(num(obj as any))
  } else if (typeof obj === 'boolean') {
    out.write(bool(obj as any))
  } else if (obj instanceof Date) {
    out.write(date(fmt.format(obj)))
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
  data: s.boolean(true),
  other: s.boolean(true)
})


@register('debug')
export class DebugTransformer extends Transformer(
  s.object({data: s.boolean(), other: s.boolean()}), s.array(s.any())
) {

  help = `Print chunks to the console.

  Note that a debug sink is always appended by default
  -- if it prints nothing it is because another
  sink handled the chunks without passing them along.`

  options_parser = DEBUG_OPTIONS
  body_parser = null

  nb = 0

  async onData(chunk: Chunk.Data) {
    // if (this.options.data) {
      this.nb++
      process.stderr.write(coll(`${chunk.collection}: ${this.nb} `))
      print_value(process.stderr, chunk.payload)
      process.stderr.write('\n')
    // }

    await this.send(chunk)
  }

}



@register('null')
export class NullSink extends Sink(s.object()) {

  help = `Lose the chunks and don't forward them.`

  async onData() {

  }

  async onExec() {

  }

}
