import {PipelineComponent, PipelineEvent} from './adapter'
import {inspect} from 'util'

export class DebugAdapter extends PipelineComponent {

  async process(event: PipelineEvent) {
    console.log(`${event.type}: ${inspect(event.payload, {colors: true, depth: null})}`)
    return event
  }

}
