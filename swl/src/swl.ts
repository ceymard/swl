#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'

export * from './adapters'

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
  'swl.oracle'
)
