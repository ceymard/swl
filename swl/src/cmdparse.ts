
import * as P from 'parsimmon'
import {PARSER as OBJ} from './oparse'

export type PipelineContent = string | string[]
export type Pipeline = PipelineContent[]

const DIVIDER = P.regex(/\s*\|\s*/)
const LBRACKET = P.regex(/\s*\[\s*/)
const RBRACKET = P.regex(/\s*]\s*/)

const SINGLE = P.regex(/[^\?]+\?/)

const INSTRUCTION = P.regex(/([^\[\]\|]+)/).map(s => {
  return s.trim()
})

const INSTR = P.alt(
  P.seqMap(LBRACKET, P.lazy(() => PIPE), RBRACKET, (_1, res, _2) => res),
  INSTRUCTION
)

const PIPE: P.Parser<Pipeline> = P.sepBy1(INSTR, DIVIDER)

export const PARSER = PIPE
