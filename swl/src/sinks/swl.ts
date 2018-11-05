import * as s from 'slz'
import { build_pipeline, PipelineComponent, instantiate_pipeline } from '../pipeline'
import { ARRAY_CONTENTS } from 'clion'
import { URI, FRAGMENTS } from '../cmdparse'

import * as P from 'parsimmon'
import {readFileSync} from 'fs'


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
    })

  try {
    return await build_pipeline(FRAGMENTS.tryParse(contents))
  } catch (e) {
    console.error(`in ${path}: ${e.message}`)
    throw e
  }
}


export const SWL_PARSER_OPTIONS = s.tuple(
  s.string().required(), // file
  s.object(), // options
  s.array(s.object())
)


export class Swl extends PipelineComponent<typeof SWL_PARSER_OPTIONS.TYPE> {

  help = `Read swl statements from a file`
  options_parser = SWL_PARSER_OPTIONS

  pipeline!: PipelineComponent<any>[]

  async init() {
    var file = await this.options[0]
    var options = this.options[1]
    var argv = this.options[2]
    this.pipeline = await build_swl_file_pipeline(file, argv || [], options)
  }

  async process() {
    var stream = instantiate_pipeline(this.pipeline, this.upstream)
    await this.forward(stream)
  }

}
