import {Adapter} from './adapter'
import {Chunk} from '../types'

export class LoggerAdapter extends Adapter {

  async handle(chunk: Chunk) {
    console.log(`log ${this.options.a}: `, chunk)
  }

}