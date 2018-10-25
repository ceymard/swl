
import { register, Transformer, ChunkIterator, Chunk } from '../pipeline'
import * as R from 'ramda'


@register('js')
export class Js extends Transformer<{}, string> {

  help = `Build a javascript function and run it.`
  options_parser = null
  body_parser = null

  R = R
  fn!: Function
  nb = 0
  current_collection = ''

  async init() {
    this.fn = eval(this.body)
  }

  async *onCollectionStart(chk: Chunk.Start): ChunkIterator {
    this.current_collection
    this.nb = 0
    yield chk
  }

  async *onData(chk: Chunk.Data): ChunkIterator {
    var res = this.fn(chk.payload, this.current_collection, this.nb++) as any
    if (res[Symbol.iterator]) {
      for (var r of res)
        yield Chunk.data(r)
    } else if (res[Symbol.asyncIterator]) {
      for await (var r of res)
        yield Chunk.data(r)
    } else yield Chunk.data(res)
  }

}


@register('jsobj')
export class JsObj extends Js {
  help = `A shortcut for a simple javascript function

  Use the '$' identifier in the body to reference the current object

  example: jsobj prop: $.otherProp.method(), prop2: $.yetAnotherProperty
  `

  async init() {
    this.fn = eval(`(_, collection, i) => { return {..._, ${this.body}} }`)
  }

}
