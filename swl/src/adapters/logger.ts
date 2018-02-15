import {Adapter, Chunk} from './adapter'

export class LoggerAdapter extends Adapter {

  async handle(chunk: Chunk) {
    console.log(`log ${this.options.a}: `, chunk)
  }

}