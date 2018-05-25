
import * as P from 'parsimmon'
import {inspect} from 'util'
import { WSAEACCES } from 'constants';

export const i = parseInt

declare module 'parsimmon' {
  interface Parser<T> {
    _name?: string
    name(str: string): this
    name(str: TemplateStringsArray): this
    tap(fn?: (res: T) => any): this
  }
}

P.Parser.prototype.tap = function <T>(
  this: P.Parser<T>,
  fn?: (res: T) => any
) {
  const f = fn || ((m: any) => console.log(inspect(m, {colors: true})))
  return this.map(res => {
    process.stdout.write((this._name || '<unknown>') + ': ')
    f(res)
    return res
  })
}

P.Parser.prototype.name = function <T>(str: string | TemplateStringsArray) {
  this._name = Array.isArray(str) ? str [0] : str
  return this
}


export const __ = P.optWhitespace
function S(t: TemplateStringsArray) {
  return P.seqMap(__, P.string(t.join('')), __, (_1, res, _2) => res)
}

export const R = P.regexp
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
}).name `Single Value`

export const re_regexp = /\/([^']+)\/([imguy]*)/

export const QUOTED = Either(
  R(/'''((?!''')[^])*'''/m).map(res => res.slice(3, -3)),
  R(/'(\\'|[^'])*'/m).map(res => res.slice(1, -1).replace(/\\'/g, "'")),
  R(/^`(\\`|[^`])*`/m).map(res => res.slice(1, -1).replace(/\\`/g, "`")),
  R(/"(\\"|[^"])*"/m).map(res => res.slice(1, -1).replace(/\\"/g, "\""))
).name `Quoted String`

export const REGEXP = R(re_regexp).map(r => {
  const [src, flags] = re_regexp.exec(r)!.slice(1)
  return new RegExp(src, flags||'')
}).name `Regexp`

export const DATE = R(re_date).map(r => {
  const [year, month, day, hh, mm, ss] = re_date.exec(r)!.slice(1)
  return new Date(i(year), i(month) - 1, i(day), i(hh)||0, i(mm)||0, i(ss)||0)
}).name `Date`

export const VALUE: P.Parser<any> = Either(
  P.lazy(() => ARRAY),
  Sequence(S`{`, P.lazy(() => OBJECT), S`}`)
    .map(([_1, res, _2]) => res),
  DATE,
  QUOTED,
  REGEXP,
  SINGLE_VALUE
).name `Value`

export const BOOL_PROP = Sequence(
  Either(
    Sequence(__, R(/not?\s+/)),
    S`!`,
    Sequence(__, R(/don'?t\s+/)),
    S``
  ),
  R(/[^,\)\]\s]+/),
).map(([no, val]) => { return {[val]: !no} })
.name `Boolean Property`

export const PROP = Sequence(
  __,
  R(/[\w\.]+/),
  S`:`,
  Either(S`,`, VALUE),
  __
).map(([__, name, _, val]) => { return {[name]: val} })
.name `Object Property`

export function SeparatedBy<T>(p: P.Parser<T>, sep: P.Parser<any>): P.Parser<T[]> {
  return Sequence(p, Sequence(sep, p).map(([_, r]) => r).many())
    .map(([first, rest]) => [first, ...rest])
}

export const OBJECT: P.Parser<{[name: string]: any}> =
  SeparatedBy(Either(PROP, BOOL_PROP), S`,`)
  .map(res => Object.assign({}, ...res))
  .name `Object`

export const ARRAY_VALUE = P.seqMap(__, P.alt(
  PROP, VALUE
), (_, v) => v).name `Array Value`

export const ARRAY_CONTENTS = P.sepBy(ARRAY_VALUE, S`,`).name `Array Contents`
export const ARRAY: P.Parser<any[]> = Sequence(S`[`, ARRAY_CONTENTS, S`]`).map(([_, ct, _2]) => ct)
  .name `Array`


export const OBJ = P.seqMap(
  OBJECT,
  // P.optWhitespace,
  P.all,
  (result, rest) => {
    return {result, rest}
  }
).name `Object and Rest`
