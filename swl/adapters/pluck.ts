
import {PipelineComponent, PipelineEvent} from './adapter'
import { register_sink } from 'swl/register';
import { ARRAY_CONTENTS } from '../lib';

export class Pluck extends PipelineComponent {

  constructor(public def: any[]) {
    super({})
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {
    const def = this.def
    for await (var ev of this.upstream()) {
      if (ev.type === 'data') {
        var obj: any = {}
        var p = ev.payload
        for (var d of def) {
          if (typeof d === 'string' || typeof d === 'number') {
            obj[d] = p[d]
          } if (d instanceof RegExp) {
            for (var x in p) {
              if (x.match(d))
                obj[x] = p[x]
            }
          }
        }
        yield this.data(obj)
      } else yield ev
    }
  }

}

register_sink(async (opts: any, rest: string) => {
  return new Pluck(ARRAY_CONTENTS.tryParse(rest))
}, 'pluck')