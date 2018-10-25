
import { Source, build_pipeline, Sink } from '../pipeline'
import { ARRAY_CONTENTS } from 'clion'
import { URI, FRAGMENTS } from '../cmdparse'
import * as P from 'parsimmon'
import {readFileSync} from 'fs'
import * as y from 'yup'


async function build_swl_file_pipeline(path: string, argv: any[], opts: any) {
  const contents = readFileSync(path, 'utf-8')
    .replace(/^#[^\n]*\n?/mg, '')
    .replace(/\$\{((?:\\\}|\\\||[^\}])+(\|(?:\\\}|[^\}])+)?)\}/g, (match, val, def) => {
      const res = argv[val]
        || opts[val]
        || def
      if (res === undefined)
        throw new Error(`in ${path}: No script argument value found for '${val}'`)
      return res
      // var fn = new Function('_', 'options', 'env', `return ${val}`)
      // return fn(argv, opts, process.env)
    })

  try {
    return await build_pipeline(FRAGMENTS.tryParse(contents))
  } catch (e) {
    console.error(`in ${path}: ${e.message}`)
    throw e
  }
}


sources.add(
  `Read swl statements from a file`,
  y.object(),
  P.seq(
    URI,
    ARRAY_CONTENTS
  ),
  async function swl(opts, [file, argv]) {
    // console.log(opts)
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