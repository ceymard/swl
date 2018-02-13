#!/usr/bin/env node
// import {PARSER} from './oparse'
import {inspect} from 'util'
import {PARSER} from './cmdparse'

// const res = PARSER.parse(`
//   no tls,
//   folder: Amgen/Pouet,
//   user: incoming@salesway.eu,
//   password: 1nc0m1ng_,
//   reg: r'.*[^]+'i,
//   files: [./c, /home/chris, /rtort*],

// no zop,
// not dope-there,
// don't discuss,
// ab,
// !acb,
// abc: pouet,
// aaa:  true  ,
// h: [1, 2, 3,  false, {toto: 2}, yes, no ],
// zob: {a: 1, b: -2.54},
// toto: 12,
// d: 2018-01-01 01:01:59


// source1: 'SELECT zobi'
// `)

const args = process.argv.slice(2).join(' ')
console.log(inspect(PARSER.parse(args), {colors: true, depth: null}))

// console.log(inspect(res.status ? res.value.result : 'error', {colors: true}))
