import {PipelineComponent, PipelineEvent} from './adapter'
import {inspect} from 'util'
import {register_sink} from 'swl/register'

export class DebugAdapter extends PipelineComponent {

  async *process() {
    var collection: string = ''
    var nb = 1
    for await (var event of this.upstream()) {
      if (event.type === 'start') {
        collection = event.name
        nb = 1
      }
      if (event.type === 'data') {
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