import {PipelineComponent, PipelineEvent} from './adapter'
import {inspect} from 'util'
import {register_sink} from 'swl/register'

export class DebugAdapter extends PipelineComponent {

  async *process() {
    for await (var event of this.upstream()) {
      console.log(`${inspect(event, {colors: true, depth: null})}`)
      yield event as PipelineEvent
    }
  }

}


register_sink(async (opts: any, str: string) => {
  return new DebugAdapter()
}, 'debug')