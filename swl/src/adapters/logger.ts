import {PipelineComponent, EventType} from './adapter'
import {inspect} from 'util'

export class DebugAdapter extends PipelineComponent {

  async process(type: EventType, payload: any) {
    console.log(`${type}: ${inspect(payload, {colors: true, depth: null})}`)
    this.send(type, payload)
  }

}

// DebugAdapter.register('debug')
