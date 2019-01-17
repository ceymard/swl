import { Transformer, Chunk, register } from '../pipeline'
import { flatten as f, unflatten as u } from 'flat'


@register('flatten')
export class Flatten extends Transformer<{}, []> {

  help = `Flatten deep-nested properties to a simple object`
  body_parser = null
  options_parser = null

  // FIXME we should make a suitable options_parser for Flatten

  async onData(chunk: Chunk.Data) {
    await this.send(Chunk.data(chunk.collection, f(chunk.payload, this.options)))
  }
}

@register('unflatten')
export class UnFlatten extends Transformer<{}, []> {

  help = `Flatten deep-nested properties to a simple object`
  body_parser = null
  options_parser = null

  // FIXME we should make a suitable options_parser for Flatten

  async onData(chunk: Chunk.Data) {
    const p = chunk.payload
    await this.send(Chunk.data(chunk.collection, u(chunk.payload, this.options)))
  }
}
