import {createWriteStream, createReadStream} from 'fs'
import {Sources} from './adapters'

export async function make_write_creator(uri: string, options: any) {
  // Check for protocol !!
  var glob = uri.indexOf('*') > -1
  var stream: NodeJS.WritableStream
  return function (name: string) {
    if (!glob && stream) return stream
    return createWriteStream(uri.replace('*', name), options)
  }
}

import * as path from 'path'

export async function *make_read_creator(uri: string, options: any): Sources {
  // Check for protocol !!
  yield {
    collection: path.basename(uri).replace(/\.[^\.]+$/, ''),
    source: createReadStream(uri, options) as NodeJS.ReadableStream
  }
}