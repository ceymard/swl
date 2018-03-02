(<any>Symbol).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator")
if (process.env.DEBUG) require('source-map-support').install()

// export * from './adapters'
export * from './cmdparse'
// export * from './register'
export * from './streams'
export * from './pipeline'
export * from './sinks'

import * as y from 'yup'
export {y, y as yup}

function try_require(...names: string[]) {
  for (var name of names) try { require(name) } catch { }
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
