import {PipelineComponent, PipelineEvent} from './adapter'
import {inspect} from 'util'

export class DebugAdapter extends PipelineComponent {

  async *process() {
    for await (var event of this.upstream()) {
      console.log(`${inspect(event, {colors: true, depth: null})}`)
      yield event as PipelineEvent
    }
  }

}
