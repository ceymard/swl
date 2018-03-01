if (process.env.DEBUG) require('source-map-support').install()

export * from './adapters'
export * from './cmdparse'
export * from './register'
export * from './streams'

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
