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

import {pipeline, DebugAdapter, Source} from './adapters'

class Test extends Source {

  constructor(public collection: string) {
    super()
  }

  async emit() {
    await this.send('start', this.collection)
    await this.send('data', {a: 1, b: 2})
    await this.send('stop', this.collection)
    await this.send('end')
  }
}

import {CsvOutput} from 'swl.csv'

var t = new Test('pouet')
var t2 = new Test('col2')
var d = new DebugAdapter()
var c = new CsvOutput({}, '%col-test.csv')
pipeline(t, t2, d, c)
