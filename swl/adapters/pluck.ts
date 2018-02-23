
import {Sink, PipelineEvent, Transformer} from './adapter'
import { register_sink } from 'swl/register';
import { ARRAY_CONTENTS } from '../lib';

export class Pluck extends Transformer {

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


export class JS extends Sink {
  static doc = `Run a function for each data`

  constructor(public fn: (a: any) => any) {
    super({})
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {
    for await (var ev of this.upstream()) {
      if (ev.type === 'data')
        yield* this.fn(ev.payload)
      else yield ev
    }
  }
}


register_sink(async (opts: any, rest: string) => {

  const fn = eval(`
    var f = function *map(o) {
      ${rest}
    };
    f
  `)

  var m = new JS(fn)
  fn.bind(m)
  return m
}, 'js')


export class On extends Sink {
  doc = `Send an exec event downstream on a specific condition`

  constructor() {
    super({})
  }

  async *process(): AsyncIterableIterator<PipelineEvent> {

  }
}

register_sink(async (opts: any, rest: string) => {
  return new On()
}, '')