
import { sinks, ChunkIterator, Chunk } from '../pipeline'
import * as y from 'yup'

sinks.add(
  y.object({}),
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


sinks.add(
  y.object({}),
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
