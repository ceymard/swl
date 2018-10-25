import { ChunkIterator, Transformer, Chunk, register } from '../pipeline'
import { flatten as f, unflatten as u } from 'flat'


@register('flatten')
export class Flatten extends Transformer<{}, []> {

  help = `Flatten deep-nested properties to a simple object`
  body_parser = null
  options_parser = null

  // FIXME we should make a suitable options_parser for Flatten

  async *onData(chunk: Chunk.Data): ChunkIterator {
    yield Chunk.data(f(chunk.payload, this.options))
  }
}

@register('unflatten')
export class UnFlatten extends Transformer<{}, []> {

  help = `Flatten deep-nested properties to a simple object`
  body_parser = null
  options_parser = null

  // FIXME we should make a suitable options_parser for Flatten

  async *onData(chunk: Chunk.Data): ChunkIterator {
    yield Chunk.data(u(chunk.payload, this.options))
  }
}
