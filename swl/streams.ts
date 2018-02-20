import {createWriteStream, createReadStream} from 'fs'

export async function make_write_creator(uri: string, options: any) {
  // Check for protocol !!
  var glob = uri.indexOf('*') > -1
  var stream: NodeJS.WritableStream
  return function (name: string) {
    if (!glob && stream) return stream
    return createWriteStream(uri.replace('*', name), options)
  }
}

export async function make_read_creator(uri: string, options: any) {
  // Check for protocol !!
  return createReadStream(uri, options) as NodeJS.ReadableStream
}