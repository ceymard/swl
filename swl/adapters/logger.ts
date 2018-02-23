import {PipelineComponent, PipelineEvent} from './adapter'
import {inspect} from 'util'
import {register_sink} from 'swl/register'
import * as y from 'yup'
import ch from 'chalk'

const c = ch.constructor({level: 3})
const prop = c.hsl(180, 30, 30)
const str = c.hsl(80, 60, 60)
const num = c.hsl(120, 60, 60)
const constant = c.hsl(0, 60, 60)
const bool = c.hsl(280, 60, 60)
const coll = c.hsl(220, 60, 60)

function print_value(out: NodeJS.WritableStream, obj: any, outside = true) {

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

export class DebugAdapter extends PipelineComponent {

  schema = y.object({
    data: y.boolean().default(true),
    other: y.boolean().default(true)
  })

  options = this.schema.cast(this.options)

  async *process() {
    var collection: string = ''
    var nb = 1
    for await (var event of this.upstream()) {
      if (event.type === 'start') {
        collection = event.name
        nb = 1
      } else if (event.type === 'data') {
        if (this.options.data) {
          process.stdout.write(coll(collection + ':' + nb++ + ' '))
          print_value(process.stdout, event.payload)
          process.stdout.write('\n')
        }
          // console.log(`${collection}:${nb++} ${inspect(event.payload, {colors: true, depth: null, breakLength: Infinity})}`)
      } else
        console.log(`${inspect(event, {colors: true, depth: null, breakLength: Infinity})}`)
      yield event as PipelineEvent
    }
  }

}


register_sink(async (opts: any, str: string) => {
  return new DebugAdapter(opts)
}, 'debug')