import {pipeline, DebugAdapter, FileSource} from 'swl'
import {InlineJson} from 'swl.json'

import {CsvOutput} from 'swl.csv'

var j = new InlineJson({coll: true}, '{"a": 1, "b": "54433dsfd"}, {"a": 5, "b": "rumplesiltskin"}')
var d = new DebugAdapter()
var c = new CsvOutput({}, '%col-test.csv')
pipeline(j, d, c)

