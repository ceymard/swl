
import * as P from 'parsimmon'

export type ParserType<T> = T extends P.Parser<infer U> ? U : never
