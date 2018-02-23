
import {Sink, PipelineEvent} from './adapter'
import { register_sink } from 'swl/register';
import { ARRAY_CONTENTS } from '../lib';

export class Pluck extends Sink {

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


export class Map extends Sink {
  constructor(public fn: (a: any) => any) {
    super({})
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {

  }
}

export class On extends Sink {
  constructor() {
    super({})
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {

  }
}

register_sink(async (opts: any, rest: string) => {
  return new On()
}, '')