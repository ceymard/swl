
import { sources, build_pipeline, sinks } from '../pipeline'
import { URI, FRAGMENTS, ARRAY_CONTENTS } from '../cmdparse'
import * as P from 'parsimmon'
import {readFileSync} from 'fs'
import * as y from 'yup'


async function build_swl_file_pipeline(path: string, argv: any[], opts: any) {
  const contents = readFileSync(path, 'utf-8')
    .replace(/^#[^\n]*\n?/mg, '')
    .replace(/\$\{((?:\\\}|[^\}])+)\}/g, (match, val) => {
      var fn = new Function('_', 'options', 'env', `return ${val}`)
      return fn(argv, opts, process.env)
    })

  return await build_pipeline(FRAGMENTS.tryParse(contents))
}


sources.add(
  `Read swl statements from a file`,
  y.object(),
  P.seq(
    URI,
    ARRAY_CONTENTS
  ),
  async function swl(opts, [file, argv]) {
    return build_swl_file_pipeline(file, argv, opts)
  },
  'swl', '.swl'
)

sinks.add(
  `Read swl statements from a file`,
  y.object(),
  P.seq(
    URI,
    ARRAY_CONTENTS
  ),
  async function swl(opts, [file, argv]) {
    return build_swl_file_pipeline(file, argv, opts)
  },
  'swl', '.swl'
)