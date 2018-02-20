import {PipelineComponent} from './adapter'
import {inspect} from 'util'

export class DebugAdapter extends PipelineComponent {

  async *process() {
    for await (var event of this.upstream()) {
      console.log(`${event.type}: ${inspect(event.payload, {colors: true, depth: null})}`)
      yield event
    }
  }

}
