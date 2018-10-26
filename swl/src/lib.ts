if (process.env.DEBUG) require('source-map-support').install()

export * from './cmdparse'
export * from './streams'
export * from './pipeline'
import './sinks'

import * as slz from './slz'
export {slz as s, slz}

export * from './types'

function try_require(...names: string[]) {
  for (var name of names) try { require(name) } catch (e) { }
}

try_require(
  'swl.json',
  'swl.csv',
  'swl.yaml',
  'swl.postgres',
  'swl.sqlite',
  'swl.imap',
  'swl.mysql',
  'swl.oracle',
  'swl.xlsx'
)
