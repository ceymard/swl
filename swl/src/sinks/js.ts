
import { transformers, ChunkIterator, Chunk } from '../pipeline'
import * as y from 'yup'

transformers.add(
`Build a javascript function and run it.`,
  y.object({}),
  null,
  function js(opts, rest) {

    var fn: Function = eval(rest)

    return async function* (upstream: ChunkIterator): ChunkIterator {
      // yield* upstream
      var collection = ''
      var i = 0
      for await (var ch of upstream) {
        if (ch.type === 'start') {
          collection = ch.name
          i = 0
          yield ch
        } else if (ch.type === 'data') {
          i++
          var res = fn(ch.payload, collection, i)
          if (res[Symbol.iterator]) {
            for (var r of res)
              yield Chunk.data(r)
          } else if (res[Symbol.asyncIterator]) {
            for await (var r of res)
              yield Chunk.data(r)
          } else yield Chunk.data(res)

        } else yield ch
      }
    }
  },
  'js'
)


transformers.add(
`A shortcut for a simple javascript function`,
  y.object({}),
  null,
  function jsobj(opts, rest) {

    var fn: Function = eval(`(_, collection, i) => { return {..._, ${rest}} }`)

    return async function* (upstream: ChunkIterator): ChunkIterator {
      // yield* upstream
      var collection = ''
      var i = 0
      for await (var ch of upstream) {
        if (ch.type === 'start') {
          collection = ch.name
          i = 0
          yield ch
        } else if (ch.type === 'data') {
          i++
          var res = fn(ch.payload, collection, i)
          if (res[Symbol.iterator]) {
            for (var r of res)
              yield Chunk.data(r)
          } else if (res[Symbol.asyncIterator]) {
            for await (var r of res)
              yield Chunk.data(r)
          } else yield Chunk.data(res)

        } else yield ch
      }
    }
  },
  'jsobj'
)
