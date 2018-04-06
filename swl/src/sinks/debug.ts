
import { sinks, ChunkIterator } from '../pipeline'
import { inspect } from 'util'
import * as y from 'yup'

import ch from 'chalk'

const c = ch.constructor({level: 3})
const prop = c.hsl(180, 30, 30)
const str = c.hsl(80, 60, 60)
const num = c.hsl(120, 60, 60)
const constant = c.hsl(0, 60, 60)
const bool = c.hsl(280, 60, 60)
const coll = c.hsl(220, 60, 60)

export function print_value(out: NodeJS.WritableStream, obj: any, outside = true) {

  if (obj == null) {
    out.write(constant(obj))
  } else if (typeof obj === 'string') {
    out.write(str(obj.replace('\n', '\\n')))
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

sinks.add(
`Print chunks to the console.

Note that a debug sink is always appended by default
-- if it prints nothing it is because another
sink handled the chunks without passing them along.`,
  y.object({
    data: y.boolean().default(true),
    other: y.boolean().default(true)
  }),
  null,
  function debug(opts) {

  return async function *(upstream: ChunkIterator): ChunkIterator {
    var collection = ''
    var nb = 0

    for await (var ch of upstream) {
      if (ch.type === 'start') {
        collection = ch.name
        nb = 1
      } else if (ch.type === 'data') {
        if (opts.data) {
          process.stdout.write(coll(collection + ':' + nb++ + ' '))
          print_value(process.stdout, ch.payload)
          process.stdout.write('\n')
        }
          // console.log(`${collection}:${nb++} ${inspect(event.payload, {colors: true, depth: null, breakLength: Infinity})}`)
      } else
        console.log(`${inspect(ch, {colors: true, depth: null, breakLength: Infinity})}`)

      yield ch
    }
  }
}, 'debug')