#!/usr/bin/env node

import {inspect} from 'util'
import {PARSER} from './cmdparse'

// const args = process.argv.slice(2).join(' ')
const args = `
  myfile.xlsx
  | spreadsheet?type: xlsx
  | sanitize
  | postgres://app:app@1.1.1.1/app?no recreate
`

console.log(inspect(PARSER.parse(args), {colors: true, depth: null}))
