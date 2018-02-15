#!/usr/bin/env node

// import {inspect} from 'util'
// import {PARSER} from './cmdparse'


// const args = process.argv.slice(2).join(' ')
// const args = `
//   myfile.xlsx
// | json://pouet {"a": 1} )
// | sanitize
// | log
// | postgres://app:app@1.1.1.1/app?no recreate
// `

// console.log(inspect(PARSER.parse(args), {colors: true, depth: null}))


import {LoggerAdapter, Adapter, JsonAdapter} from './adapters'
// import {Readable} from 'stream'

// var r = new Readable({objectMode: true})
// var r2 = new Readable({objectMode: true})
const j = new JsonAdapter('pouet', {name: 'col1'}, '{"a": 1, "b": 2}, {"a": 3, "b": 4}')
const j2 = new JsonAdapter('col2', {name: 'col2'}, '{"a": 5, "b": 6}, {"a": 7, "b": 8}')
const j3 = new JsonAdapter('test.json')
var l = new LoggerAdapter('logger', {a: 1})
// var l2 = new LoggerAdapter({a: 2})

Adapter.pipeline(
  j, j2, j3, l
)


// l.push('pouet')
// for (var i = 0; i < 4; i++) {
//   r.push({i, obj: 'something'})
// }
// r.push(null)
