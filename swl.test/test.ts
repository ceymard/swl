import {pipeline, DebugAdapter} from 'swl'
import {InlineJson} from 'swl.json'
import * as fs from 'fs'

// (Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

import {CsvOutput} from 'swl.csv'
import {SqliteSource, SqliteSink} from 'swl.sqlite'

var j = new InlineJson({coll: true}, '{"a": 1, "b": "54433dsfd"}, {"a": 5, "b": "rumplesiltskin"}')
var j2 = new InlineJson({colll: true}, '{"a": 1, "b": "sdfsdf"}, {"a": 5, "b": "dfdskjn"}')
var d = new DebugAdapter()
var s = new SqliteSource('test.db', {}, {
  visites_plus: 'visites',
  visites_n101: `select * from "visites" where lower(secteur) = 'n0101'`
})
var c = new CsvOutput({}, (name: string) => fs.createWriteStream(`${name}.csv`));
var ss = new SqliteSink('output.db', {truncate: true})
pipeline(j, j2, s, d, ss).catch(e => {
  console.error(e.stack)
})
