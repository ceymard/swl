import {PipelineComponent, PipelineEvent} from './adapter'
import {inspect} from 'util'
import {register_sink} from 'swl/register'
import * as y from 'yup'

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
      }
      if (event.type === 'data') {
        if (this.options.data)
          console.log(`${collection}:${nb++} ${inspect(event.payload, {colors: true, depth: null, breakLength: Infinity})}`)
      } else
        console.log(`${inspect(event, {colors: true, depth: null, breakLength: Infinity})}`)
      yield event as PipelineEvent
    }
  }

}


register_sink(async (opts: any, str: string) => {
  return new DebugAdapter(opts)
}, 'debug')