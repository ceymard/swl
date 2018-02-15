#!/usr/bin/env node

import {inspect} from 'util'
import {PARSER} from './cmdparse'


// const args = process.argv.slice(2).join(' ')
const args = `
( myfile.xlsx :: json://?(collection:pouet) {"a": 1} )
| sanitize
| log
| postgres://app:app@1.1.1.1/app?no recreate
`

console.log(inspect(PARSER.parse(args), {colors: true, depth: null}))


import {LoggerAdapter, Adapter} from './adapters'
import {Readable, Writable} from 'stream'

class Test extends Readable {
  _read() {
    // console.log('???')
  }
}

var r = new Test({objectMode: true})
var l = new LoggerAdapter({a: 1})
var l2 = new LoggerAdapter({a: 2})

Adapter.pipeline(
  r, l, l2
)

// l.push('pouet')
for (var i = 0; i < 8; i++) {
  r.push(`pouet${i}`)
}
