
import { build_pipeline, PipelineComponent, instantiate_pipeline, register } from '../pipeline'
import { ARRAY_CONTENTS } from 'clion'
import { URI, FRAGMENTS } from '../cmdparse'
import { ParserType } from '../types'

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
  console.log(contents)

  try {
    const pipeline = await build_pipeline(FRAGMENTS.tryParse(contents))
    return pipeline
  } catch (e) {
    console.error(`in ${path}: ${e.message}`)
    throw e
  }
}


export const SWL_PARSER = P.seq(
  URI,
  ARRAY_CONTENTS
)

@register('swl', '.swl')
export class Swl extends PipelineComponent<{}, ParserType<typeof SWL_PARSER>> {

  help = `Read swl statements from a file`
  options_parser = null
  body_parser = SWL_PARSER

  pipeline!: PipelineComponent<any, any>[]

  async init() {
    var file = await this.body[0]
    var argv = this.body[1]
    this.pipeline = await build_swl_file_pipeline(file, argv, this.options)
  }

  async process() {
    var stream = instantiate_pipeline(this.pipeline, this.upstream)
    await this.forward(stream)
    await this.send(null)
  }

}
