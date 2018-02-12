#!/usr/bin/env node
import {PARSER} from './oparse'

console.log(PARSER.parse(`
  nocaca

  nozop,
  ab,
  !acb,
  abc: pouet,
  aaa:  true  ,
  h: [1, 2, 3,  false ],
  zob:(a: 1, b: 2),
  toto: 12,
  d: 2018-01-01 01:01:59


  source1: 'SELECT zobi'
`))
