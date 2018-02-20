
import * as P from 'parsimmon'
const i = parseInt

function S(t: TemplateStringsArray) {
  return P.seqMap(P.optWhitespace, P.string(t.join('')), P.optWhitespace, (_1, res, _2) => res)
}

const R = P.regexp


const re_date = /(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?/
const re_number = /^\s*-?\d+(\.\d+)?\s*$/
const SINGLE_VALUE = R(/[^,\}\]\s]+/).map(res => {
  const num = re_number.exec(res)
  if (num) {
    return parseFloat(num[0])
  }

  return res
})

const re_regexp = /r'([^']+)'([imguy]*)/

const QUOTED = R(/'[^']*'|"[^"]*"/).map(res => res.slice(1, -1))
const TRUE = P.alt(S`true`, S`yes`).map(_ => true)
const FALSE = P.alt(S`false`, S`no`).map(_ => false)
const REGEXP = R(re_regexp).map(r => {
  const [src, flags] = re_regexp.exec(r)!.slice(1)
  return new RegExp(src, flags||'')
})

const DATE = R(re_date).map(r => {
  const [year, month, day, hh, mm, ss] = re_date.exec(r)!.slice(1)
  return new Date(i(year), i(month) - 1, i(day), i(hh)||0, i(mm)||0, i(ss)||0)

})

const VALUE: P.Parser<any> = P.alt(
  P.seqMap(S`[`, P.sepBy(P.lazy(() => VALUE), S`,`), S`]`, (_1, res, _2) => res),
  P.seqMap(S`{`, P.lazy(() => OBJECT), S`}`, (_1, res, _2) => res),
  TRUE,
  FALSE,
  DATE,
  QUOTED,
  REGEXP,
  SINGLE_VALUE
)

const BOOL_PROP = P.seqMap(
  P.alt(
    P.seq(P.optWhitespace, R(/not?\s+/)),
    S`!`,
    P.seq(P.optWhitespace, R(/don'?t\s+/)),
    S``
  ),
  R(/[^,\)\]\s]+/),
  (no, val) => { return {[val]: !no} }
)

const PROP = P.seqMap(
  R(/\w+/),
  S`:`,
  VALUE,
  (name, _, val) => { return {[name]: val} }
)

// const value = P.any(date, prop)

export const OBJECT: P.Parser<{[name: string]: any}> =
  P.sepBy1(P.alt(PROP, BOOL_PROP), S`,`)
  .map(res => Object.assign({}, ...res))


export const OBJ = P.seqMap(
  OBJECT,
  // P.optWhitespace,
  P.all,
  (result, rest) => {
    return {result, rest}
  }
)
export type PipelineContent = string | string[]
export type Pipeline = PipelineContent[]

const DIVIDER = S`|`
const SOURCE_DIVIDER = S`|<`

// const SINGLE = P.regex(/[^\?]+\?/)

const INSTRUCTION = P.regex(/([^\[\]\|]+)/).map(s => {
  return s.trim()
})

const SOURCE = P.seqMap(SOURCE_DIVIDER, INSTRUCTION, (_, inst) => { return {type: 'source', inst} })
const SINK = P.seqMap(
  DIVIDER,
  INSTRUCTION,
  (_, inst) => { return {type: 'sink', inst} }
)

const INST = P.alt(SOURCE, SINK)

const PIPE = P.seqMap(
  INSTRUCTION.map(inst => { return {type: 'source', inst} }),
  INST.many(),
  (i, m) => [i, ...m]
)

export const URI = P.regex(/(\\\?|\\\s|[^\?\s]+)/)

export const URI_AND_OBJ = P.seqMap(
  P.optWhitespace,
  URI,
  P.optWhitespace,
  OBJECT.atMost(1),
  P.optWhitespace,
  (_, uri, _2, obj) => [uri, obj[0]] as [string, any]
)

export const URI_WITH_OPTS = P.seqMap(
  P.optWhitespace,
  URI,
  P.alt(
    P.seqMap(S`?`, OBJECT, P.optWhitespace ,(_q, ob) => ob),
    P.optWhitespace.map(_ => null)
  ),
  (_1, uri, opts) => [uri, opts || {}] as [string, {[name: string]: any}]
)

export const PARSER = PIPE

export const ADAPTER_AND_OPTIONS = P.seqMap(
  P.optWhitespace,
  URI,
  P.alt(
    P.seqMap(S`?`, OBJECT, P.optWhitespace ,(_q, ob) => ob),
    P.optWhitespace.map(_ => null)
  ),
  P.all,
  (_1, uri, opts, rest) => [uri, opts || {}, rest] as [string, {[name: string]: any}, string]
)

export function parse(...parsers: P.Parser<any>[]) {

}
