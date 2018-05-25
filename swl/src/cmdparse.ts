
import * as P from 'parsimmon'
import { OBJECT, QUOTED } from 'clion'
import { open_tunnel } from './streams'

function S(t: TemplateStringsArray) {
  return P.seqMap(P.optWhitespace, P.string(t.join('')), P.optWhitespace, (_1, res, _2) => res)
}

const R = P.regexp
const __ = P.optWhitespace

export const Either = P.alt

function _Sequence(...a: P.Parser<any>[]) {
  const parsers = [] as P.Parser<any>[]
  var cbk: Function | null = null
  for (var p of a) {
    if (typeof p === 'function') {
      cbk = p
      break
    } else {
      parsers.push(__)
      parsers.push(p)
    }
  }

  return P.seq(...parsers).map(res => {
    res = res.filter((item, idx) => idx % 2 !== 0)
    return cbk ? cbk(...res) : res
  })
}

export const Sequence = _Sequence as typeof P.seq & typeof P.seqMap

export function Optional<T>(p: P.Parser<T>): P.Parser<T | null> {
  return Sequence(__, p, (_, r) => r)
    .atMost(1).map(r => (r[0] || null))
}


export type PipelineContent = string | string[]
export type Pipeline = PipelineContent[]
export type Fragment = {
  type: 'source' | 'sink',
  inst: string
}

const DIVIDER = S`::`
const SOURCE_DIVIDER = S`++`

// const SINGLE = P.regex(/[^\?]+\?/)

const INSTRUCTION =  R(/((?!::|\+\+)[^])+/) // AnythingBut(DIVIDER, SOURCE_DIVIDER)

const SOURCE = Sequence(
  SOURCE_DIVIDER,
  INSTRUCTION,
  (_, inst) => { return {type: 'source', inst} as Fragment }
)


const SINK = Sequence(
  DIVIDER,
  INSTRUCTION,
  (_, inst) => { return {type: 'sink', inst} as Fragment }
)

const INST = P.alt(SOURCE, SINK)

const PIPE = P.seqMap(
  INSTRUCTION.map(inst => { return {type: 'source', inst} as Fragment }),
  INST.many(),
  (i, m) => i.inst.trim() ? [i, ...m] : m
)

// const SPACE = R(/\s/)
const OPTS_MARKER = R(/[%\?]/)

export const URI = Either(
  // QUOTED,
  R(/[^\s\n\r \t%\?]+/m)
).map(u => {
  return open_tunnel(u)
})



export const URI_WITH_OPTS = P.seqMap(
  __,
  URI,
  Either(
    P.seqMap(OPTS_MARKER, OBJECT, P.optWhitespace, (_q, ob) => ob),
    __.map(_ => null)
  ),
  (_1, uri, opts) => [uri, opts || {}] as [Promise<string>, {[name: string]: any}]
)


export const STREAM = Sequence(
  URI,
  Optional(Sequence(OPTS_MARKER, OBJECT, (_, ob) => ob)),
)

/**
 * A stream wrapper generator function
 */
export const WRITE_STREAMS = STREAM

/**
 * Stream wrappers for several reads (when taking globbing into account)
 */
export const READ_STREAMS = STREAM


export const FRAGMENTS = PIPE

export const ADAPTER_AND_OPTIONS = P.seqMap(
  __,
  URI,
  Either(
    P.seqMap(OPTS_MARKER, OBJECT, __, (_q, ob) => ob),
    __.map(_ => null)
  ),
  P.all,
  (_1, uri, opts, rest) => {
    return [uri, opts || {}, rest] as [Promise<string>, {[name: string]: any}, string]
  }
)


export const OPT_OBJECT = Optional(OBJECT).map(o => o || {})

export {OBJECT, QUOTED}