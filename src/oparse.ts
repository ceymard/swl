import * as P from 'parsimmon'

const i = parseInt
const date_reg = /(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?/

function S(t: TemplateStringsArray) {
  return P.seqMap(P.optWhitespace, P.string(t.join('')), P.optWhitespace, (_1, res, _2) => res)
}

const R = P.regexp


const DATE = R(date_reg)
  .map(res => {
    const [_, year, month, day, hh, mm, ss] = date_reg.exec(res)!
    console.log(year)
    return new Date(i(year), i(month) - 1, i(day), i(hh)||0, i(mm)||0, i(ss)||0)
  })

////////////////////////////////////////////////

const STR = R(/'[^']*'|"[^"]*"|[^,\)\]]+/)
const TRUE = P.alt(S`true`, S`yes`).map(_ => true)
const FALSE = P.alt(S`false`, S`no`).map(_ => false)
const NUMBER = R(/\d+(\.\d+)?/).map(Number)

const VALUE: P.Parser<any> = P.alt(
  P.seqMap(S`[`, P.sepBy(P.lazy(() => VALUE), S`,`), S`]`, (_1, res, _2) => res),
  P.seqMap(S`(`, P.lazy(() => OBJECT), S`)`, (_1, res, _2) => res),
  DATE,
  TRUE,
  FALSE,
  NUMBER,
  STR,
)

const BOOL_PROP = P.seqMap(
  P.alt(S`no`, S`!`, S`dont`, S``),
  R(/\w+/),
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