import {Adapter, Chunk} from './adapter'
import {inspect} from 'util'

export class DebugAdapter extends Adapter<{}> {

  async handle(chunk: Chunk) {
    console.log(`${chunk.type}: ${inspect(chunk.payload, {colors: true, depth: null})}`)
  }

}

DebugAdapter.register('debug')
