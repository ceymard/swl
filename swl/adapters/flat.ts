import {PipelineEvent, Transformer} from './adapter'
import { register_sink } from 'swl/register';
import { flatten, unflatten } from 'flat'

export class Flatten extends Transformer {

  constructor() {
    super({})
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {
    for await (var ev of this.upstream()) {
      if (ev.type === 'data') {
        var p = ev.payload
        yield this.data(flatten(p))
      } else yield ev
    }
  }

}

register_sink(async () => {
  return new Flatten()
}, 'flatten')


export class Unflatten extends Transformer {

  constructor() {
    super({})
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {
    for await (var ev of this.upstream()) {
      if (ev.type === 'data') {
        var p = ev.payload
        yield this.data(unflatten(p))
      } else yield ev
    }
  }

}

register_sink(async () => {
  return new Unflatten()
}, 'unflatten')