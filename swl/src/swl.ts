#!/usr/bin/env node

import {PARSER} from './cmdparse'
import {PARSER as OPARSER} from './oparse'
import {Adapter, registry} from './adapters'

export * from './adapters'

function try_require(...names: string[]) {
  for (var name of names) try { require(name) } catch { }
}

try_require(
  'swl.json',
  'swl.csv',
  'swl.postgres',
  'swl.sqlite'
)

const args = `
  json://col1 {"a": 1, "b": 2}, {"a": 3, "b": 4}
| json://col2 {"a": 5, "b": 6}, {"a": 7, "b": 8}
| csv://test-%col.csv?beautify,object,delimiter:;
`
const parse = PARSER.parse(args)

const re_inst = /^(?:(\w+):\/\/)?([^?\s]*)(\?)?([^]*)/

if (parse.status) {
  const arr = parse.value as string[]
  const pipeline = arr.map(e => {
    const m = re_inst.exec(e)
    if (!m) throw new Error('!')
    var [_, protocol, uri, inter, rest] = m!
    _
    var options = {}
    var body = rest || ''
    uri = uri || ''
    var in_registry = protocol || uri

    if (inter) {
      const res = OPARSER.parse(rest)
      if (res.status) {
        options = res.value.result
        body = res.value.rest
      } else {
        throw new Error('wrong arguments')
      }
    }

    const adapter = registry[in_registry]
    if (!adapter) {
      throw new Error(`No adapter found for ${in_registry}`)
    }
    // console.log(adapter.name, uri, options, body)
    return new adapter(uri, options, body)
    // console.log(in_registry, options, body)
  })

  Adapter.pipeline(...pipeline)
  // console.log(arr)
}

// process.exit(0)

// import {DebugAdapter, Adapter, JsonAdapter} from './adapters'
// // import {Readable} from 'stream'

// // var r = new Readable({objectMode: true})
// // var r2 = new Readable({objectMode: true})
// const j = new JsonAdapter('pouet', {}, '{"a": 1, "b": 2}, {"a": 3, "b": 4}')
// const j2 = new JsonAdapter('col2', {}, '{"a": 5, "b": 6}, {"a": 7, "b": 8}')
// const j3 = new JsonAdapter('test.json', {beautify: true, object: true})
// var l = new DebugAdapter('logger')
// // var l2 = new LoggerAdapter({a: 2})

// Adapter.pipeline(
//   j, j2, l, j3
// )


// // l.push('pouet')
// // for (var i = 0; i < 4; i++) {
// //   r.push({i, obj: 'something'})
// // }
// // r.push(null)
