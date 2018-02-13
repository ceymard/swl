#!/usr/bin/env node

import {inspect} from 'util'
import {PARSER} from './cmdparse'

const args = process.argv.slice(2).join(' ')
console.log(inspect(PARSER.parse(args), {colors: true, depth: null}))
