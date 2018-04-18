
import * as P from 'parsimmon'

export const i = parseInt

function S(t: TemplateStringsArray) {
  return P.seqMap(P.optWhitespace, P.string(t.join('')), P.optWhitespace, (_1, res, _2) => res)
}

export const R = P.regexp
export const __ = P.optWhitespace
export const Either = P.alt
export const Sequence = P.seq

export const re_date = /(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?/
export const re_number = /^\s*-?\d+(\.\d+)?\s*$/
export const SINGLE_VALUE = R(/[^,\}\]\s]+/).map(res => {
  const num = re_number.exec(res)
  if (num) {
    return parseFloat(num[0])
  }
  var t = res.trim()
  if (t === 'true') return true
  if (t === 'false') return false
  return res
})

export const re_regexp = /\/([^']+)\/([imguy]*)/

export const QUOTED = Either(
  R(/'''((?!''')[^])*'''/m).map(res => res.slice(3, -3)),
  R(/'(\\'|[^'])*'/).map(res => res.slice(1, -1).replace(/\\'/g, "'")),
  R(/"(\\"|[^"])*"/m).map(res => res.slice(1, -1).replace(/\\"/g, "'"))
)

export const REGEXP = R(re_regexp).map(r => {
  const [src, flags] = re_regexp.exec(r)!.slice(1)
  return new RegExp(src, flags||'')
})

export const DATE = R(re_date).map(r => {
  const [year, month, day, hh, mm, ss] = re_date.exec(r)!.slice(1)
  return new Date(i(year), i(month) - 1, i(day), i(hh)||0, i(mm)||0, i(ss)||0)

})

export const VALUE: P.Parser<any> = Either(
  P.lazy(() => ARRAY),
  Sequence(S`{`, P.lazy(() => OBJECT), S`}`)
    .map(([_1, res, _2]) => res),
  DATE,
  QUOTED,
  REGEXP,
  SINGLE_VALUE
)

export const BOOL_PROP = Sequence(
  Either(
    Sequence(__, R(/not?\s+/)),
    S`!`,
    Sequence(__, R(/don'?t\s+/)),
    S``
  ),
  R(/[^,\)\]\s]+/),
).map(([no, val]) => { return {[val]: !no} })

export const PROP = Sequence(
  R(/[\w\.]+/),
  S`:`,
  Either(S`,`, VALUE)
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
