import * as P from 'parsimmon'

const i = parseInt
const date_reg = /^\s*(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?\s*$/
const re_number = /^\s*-?\d+(\.\d+)?\s*$/

function S(t: TemplateStringsArray) {
  return P.seqMap(P.optWhitespace, P.string(t.join('')), P.optWhitespace, (_1, res, _2) => res)
}

const R = P.regexp


const IDENT = R(/[^,\}\]\s]+/).map(res => {
  const date_match = date_reg.exec(res)
  if (date_match) {
    const [_, year, month, day, hh, mm, ss] = date_match
    return new Date(i(year), i(month) - 1, i(day), i(hh)||0, i(mm)||0, i(ss)||0)
  }

  const num = re_number.exec(res)
  if (num) {
    return parseFloat(num[0])
  }

  return res
})

const QUOTED = R(/'[^']*'|"[^"]*"|`[^`]*`/).map(res => res.slice(1, -1))
const TRUE = P.alt(S`true`, S`yes`).map(_ => true)
const FALSE = P.alt(S`false`, S`no`).map(_ => false)

const VALUE: P.Parser<any> = P.alt(
  P.seqMap(S`[`, P.sepBy(P.lazy(() => VALUE), S`,`), S`]`, (_1, res, _2) => res),
  P.seqMap(S`{`, P.lazy(() => OBJECT), S`}`, (_1, res, _2) => res),
  TRUE,
  FALSE,
  QUOTED,
  IDENT
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


export const PARSER = P.seqMap(
  OBJECT,
  // P.optWhitespace,
  P.all,
  (result, rest) => {
    return {result, rest}
  }
)