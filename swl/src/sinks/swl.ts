
import { sources, build_pipeline, sinks } from '../pipeline'
import { URI, FRAGMENTS } from '../cmdparse'
import {readFileSync} from 'fs'
import * as y from 'yup'


async function build_swl_file_pipeline(path: string, opts: any) {
  const contents = readFileSync(path, 'utf-8')
    .replace(/^#[^\n]*\n?/mg, '')
    .replace(/\$\{((?:\\\}|[^\}])+)\}/g, (match, val) => {
      var fn = new Function('_', `return ${val}`)
      return fn(opts)
    })
    // console.log(contents)
  return await build_pipeline(FRAGMENTS.tryParse(contents))
}


sources.add(
  `Read swl statements from a file`,
  y.object(),
  URI,
  async function swl(opts, file) {
    return build_swl_file_pipeline(file, opts)
  },
  'swl', '.swl'
)

sinks.add(
  `Read swl statements from a file`,
  y.object(),
  URI,
  async function swl(opts, file) {
    return build_swl_file_pipeline(file, opts)
  },
  'swl', '.swl'
)