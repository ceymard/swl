
import * as P from 'parsimmon'
import { OBJECT, QUOTED } from 'clion'
import { open_tunnel } from './streams'

function S(t: TemplateStringsArray) {
  return P.seqMap(P.optWhitespace, P.string(t.join('')), P.optWhitespace, (_1, res, _2) => res)
}

const R = P.regexp
const __ = P.optWhitespace

function AnythingBut(...parsers: P.Parser<any>[]): P.Parser<string> {
  return P.seq(...parsers.map(p => P.notFollowedBy(p)), P.any)
    .map((res: any[]) => res[res.length - 1] as string)
    .many().map(s => s.join(''))
}

export const Either = P.alt
export const Sequence = P.seq
export function Optional<T>(p: P.Parser<T>): P.Parser<T | null> {
  return p.atMost(1).map(r => (r[0] || null))
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

const INSTRUCTION = AnythingBut(DIVIDER, SOURCE_DIVIDER)

const SOURCE = P.seqMap(SOURCE_DIVIDER, INSTRUCTION, (_, inst) => { return {type: 'source', inst} as Fragment })
const SINK = P.seqMap(
  DIVIDER,
  INSTRUCTION,
  (_, inst) => { return {type: 'sink', inst} as Fragment }
)

const INST = Either(SOURCE, SINK)

const PIPE = P.seqMap(
  INSTRUCTION.map(inst => { return {type: 'source', inst} as Fragment }),
  INST.many(),
  (i, m) => i.inst.trim() ? [i, ...m] : m
)

const SPACE = R(/\s/)
const OPTS_MARKER = R(/[%\?]/)

export const URI = Either(QUOTED, AnythingBut(SPACE, OPTS_MARKER)).map(u => open_tunnel(u))


export const URI_WITH_OPTS = P.seqMap(
  __,
  URI,
  Either(
    P.seqMap(OPTS_MARKER, OBJECT, P.optWhitespace ,(_q, ob) => ob),
    __.map(_ => null)
  ),
  (_1, uri, opts) => [uri, opts || {}] as [Promise<string>, {[name: string]: any}]
)

export const FRAGMENTS = PIPE

export const ADAPTER_AND_OPTIONS = P.seqMap(
  P.optWhitespace,
  URI,
  Either(
    P.seqMap(OPTS_MARKER, OBJECT, P.optWhitespace ,(_q, ob) => ob),
    P.optWhitespace.map(_ => null)
  ),
  P.all,
  (_1, uri, opts, rest) => [uri, opts || {}, rest] as [Promise<string>, {[name: string]: any}, string]
)


export const OPT_OBJECT = Optional(OBJECT).map(o => o || {})

export {OBJECT, QUOTED}