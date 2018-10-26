import {createWriteStream, createReadStream} from 'fs'
// import {Sources} from './adapters'

export async function make_write_creator(uri: string, options: any) {
  // Check for protocol !!
  var glob = uri.indexOf('*') > -1
  var stream: StreamWrapper<NodeJS.WritableStream>
  return async function (name: string) {
    if (!glob && stream) return stream
    stream = new StreamWrapper(createWriteStream(uri.replace('*', name), options))
    return stream
  }
}

import * as path from 'path'

export function *make_read_creator(uri: string, options: any) {
  // Check for protocol !!
  yield {
    collection: path.basename(uri).replace(/\.[^\.]+$/, ''),
    source: createReadStream(uri, options) as NodeJS.ReadableStream
  }
}


export class Lock<T = void> {
  private _prom: Promise<T> | null = null
  private _resolve: null | ((val: T) => void) = null
  private _reject: null | ((err: any) => void) = null

  get promise(): Promise<T> {
    if (!this._prom) {
      this._prom = new Promise<T>((resolve, reject) => {
        this._resolve = resolve
        this._reject = reject
      })
    }
    return this._prom
  }

  private reset() {
    this._prom = null
    this._resolve = null
    this._reject = null
  }

  resolve(this: Lock<void>): void
  resolve(value: T): void
  resolve(value?: T) {
    if (!this._resolve) {
      // this.promise
      return
    }
    const res = this._resolve!
    this.reset()
    res(value!)
  }

  reject(err: any) {
    if (!this._reject) { this.promise }
    const rej = this._reject!
    this.reset()
    rej(err)
  }
}



export class StreamWrapper<T extends NodeJS.ReadableStream | NodeJS.WritableStream> {

  _ended: boolean = false
  should_drain = false
  ended = new Lock()
  readable = new Lock()
  drained = new Lock()

  constructor(public stream: T, public collection?: string) {
    stream.on('readable', e => this.readable.resolve())
    stream.on('end', e => {
      this._ended = true
      this.ended.resolve()
      this.readable.resolve()
    })
    stream.on('drain', e => {
      this.drained.resolve()
    })
    stream.on('error', e => {
      this.ended.reject(e)
      this.readable.reject(e)
      this.drained.reject(e)
    })
  }

  /**
   *
   */
  async read<U extends NodeJS.ReadableStream>(this: StreamWrapper<U>) {
    do {
      var res = this.stream.read()

      if (res !== null)
        return res

      if (this._ended) {
        // console.log('stream ended')
        return null
      }
      await this.readable.promise
    } while (true)

  }

  /**
   * Write data to the streama
   */
  async write<U extends NodeJS.WritableStream>(this: StreamWrapper<U>, data: any) {

    if (this.should_drain) {
      await this.drained.promise
      this.should_drain = false
    }

    if (!this.stream.write(data))
      this.should_drain = true
  }

  async close<U extends NodeJS.WritableStream>(this: StreamWrapper<U>) {
    this.stream.end()
    if (!this._ended) await this.ended.promise
  }

}


import * as gp from 'get-port'
const tunnel = require('tunnel-ssh')
const conf = require('ssh-config')
import * as fs from 'fs'
import { promisify } from 'util'

/**
 * Try to find a forward pattern in an URI and create the ssh tunnel if
 * found.
 *
 * @param uri: the uri to search the pattern in
 * @returns a modified URI with the forwarded port on localhost
 */
export async function open_tunnel(uri: string) {
  var local_port = await gp()
  var re_tunnel = /([^@:\/]+):(\d+)@@(?:([^@:]+)(?::([^@]+))?@)?([^:/]+)(?::([^\/]+))?/

  var match = re_tunnel.exec(uri)

  // If there is no forward to create, just return the uri as-is
  if (!match) return uri

  const [remote_host, remote_port, user, password, host, port] = match.slice(1)

  var config: any = {
    host, port: port,
    dstHost: remote_host, dstPort: remote_port,
    localPort: local_port, localHost: '127.0.0.1'
  }

  if (user) config.username = user
  if (password) config.password = password

  try {
    var _conf = conf.parse(fs.readFileSync(`${process.env.HOME}/.ssh/config`, 'utf-8'))
    const comp = _conf.compute(host)
    if (comp.HostName) config.host = comp.HostName
    if (comp.User && !config.username) config.username = comp.User
    if (comp.Password && !config.password) config.password = comp.Password
    if (comp.Port && !config.port) config.port = comp.Port

  } catch (e) {
    // console.log(e)
  }

  if (!config.port) config.port = 22

  // Create the tunnel
  await promisify(tunnel)(config)
  return uri.replace(match[0], `127.0.0.1:${local_port}`)
}
