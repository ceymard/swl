if (process.env.DEBUG) require('source-map-support').install()

Date.prototype.toString = function toString(this: Date) {
  // Why oh why does Date have a default toString that is so useless ?
  return this.toISOString()
}

export * from './cmdparse'
export * from './streams'
export * from './pipeline'
import './sinks'

import * as slz from 'slz'
export {slz as s, slz}

export * from './types'

function try_require(...names: string[]) {
  for (var name of names) try { require(name) } catch (e) { console.log(e.stack) }
}

try_require(
  // 'swl.json',
  'swl.csv',
  // 'swl.yaml',
  'swl.postgres',
  'swl.sqlite',
  // 'swl.imap',
  // 'swl.mysql',
  // 'swl.oracle',
  'swl.xlsx',
  'swl.mssql'
)
