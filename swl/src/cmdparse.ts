
import * as P from 'parsimmon'
const i = parseInt

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

const Either = P.alt
const Sequence = P.seq


const re_date = /(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?/
const re_number = /^\s*-?\d+(\.\d+)?\s*$/
const SINGLE_VALUE = R(/[^,\}\]\s]+/).map(res => {
  const num = re_number.exec(res)
  if (num) {
    return parseFloat(num[0])
  }
  var t = res.trim()
  if (t === 'yes' || t === 'true') return true
  if (t === 'no' || t === 'false') return false
  return res
})

const re_regexp = /\/([^']+)\/([imguy]*)/

const QUOTED = Either(
  R(/'''((?!''')[^])*'''/m).map(res => res.slice(3, -3)),
  R(/'(\\'|[^'])*'/).map(res => res.slice(1, -1).replace(/\\'/g, "'")),
  R(/"(\\"|[^"])*"/m).map(res => res.slice(1, -1).replace(/\\"/g, "'"))
)

const REGEXP = R(re_regexp).map(r => {
  const [src, flags] = re_regexp.exec(r)!.slice(1)
  return new RegExp(src, flags||'')
})

const DATE = R(re_date).map(r => {
  const [year, month, day, hh, mm, ss] = re_date.exec(r)!.slice(1)
  return new Date(i(year), i(month) - 1, i(day), i(hh)||0, i(mm)||0, i(ss)||0)

})

const VALUE: P.Parser<any> = Either(
  P.lazy(() => ARRAY),
  Sequence(S`{`, P.lazy(() => OBJECT), S`}`)
    .map(([_1, res, _2]) => res),
  DATE,
  QUOTED,
  REGEXP,
  SINGLE_VALUE
)

const BOOL_PROP = Sequence(
  Either(
    Sequence(__, R(/not?\s+/)),
    S`!`,
    Sequence(__, R(/don'?t\s+/)),
    S``
  ),
  R(/[^,\)\]\s]+/),
).map(([no, val]) => { return {[val]: !no} })

const PROP = Sequence(
  R(/[\w\.]+/),
  S`:`,
  VALUE
).map(([name, _, val]) => { return {[name]: val} })

// const value = P.any(date, prop)

export const OBJECT: P.Parser<{[name: string]: any}> =
  P.sepBy1(Either(PROP, BOOL_PROP), S`,`)
  .map(res => Object.assign({}, ...res))

export const ARRAY_VALUE = P.seqMap(__, P.alt(
  PROP, VALUE
), (_, v) => v)

export const ARRAY_CONTENTS = P.sepBy(ARRAY_VALUE, S`,`)
export const ARRAY: P.Parser<any[]> = Sequence(S`[`, ARRAY_CONTENTS, S`]`).map(([_, ct, _2]) => ct)


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
  (i, m) => [i, ...m]
)

const SPACE = R(/\s/)
const OPTS_MARKER = R(/%/)

export const URI = AnythingBut(SPACE, OPTS_MARKER)

export const URI_AND_OBJ = P.seqMap(
  __,
  URI,
  __,
  OBJECT.atMost(1),
  __,
  (_, uri, _2, obj) => [uri, obj[0]||{}] as [string, any]
)

export const URI_WITH_OPTS = P.seqMap(
  __,
  URI,
  Either(
    P.seqMap(OPTS_MARKER, OBJECT, P.optWhitespace ,(_q, ob) => ob),
    __.map(_ => null)
  ),
  (_1, uri, opts) => [uri, opts || {}] as [string, {[name: string]: any}]
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
  (_1, uri, opts, rest) => [uri, opts || {}, rest] as [string, {[name: string]: any}, string]
)

export function parse(...parsers: P.Parser<any>[]) {

}
