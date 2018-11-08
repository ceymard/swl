#!/usr/bin/env node

import {FRAGMENTS, instantiate_pipeline, build_pipeline, sources, sinks, transformers} from './lib'
// import { readFileSync } from 'fs'
import chalk from 'chalk'
import { PipelineComponent } from './pipeline';

// console.log(process.argv)
const args = process.argv.slice(2)
// console.log(args)

function renderDoc(obj: PipelineComponent) {
  const marked = require('marked')
  const TerminalRenderer = require('marked-terminal')

  marked.setOptions({
    renderer: new TerminalRenderer()
  })

  // const schema = obj.schema as yup.ObjectSchema<any>
  // console.log(schema.describe())
  console.log(marked(obj.help))
}

function displaySimple(obj: PipelineComponent, mimes: string[], color: ((s: string) => string)) {
  const first = obj.help.split('\n').map(l => l.trim()).filter(id => id)[0]
  console.log(`  - ${color(mimes[0])}${mimes.length > 1 ? ' ' : ''}${chalk.gray(mimes.slice(1).join(', '))} ${first}`)
  // renderDoc(obj)
}

async function run() {

  const contents = args
  .map(a => a.indexOf(' ') > -1 ? `'${a.replace(/'/g, "\\'")}'` : a)
  .join(' ')

  const fragments = FRAGMENTS.tryParse(contents)

  // Display help about components.
  if (fragments.length < 1 || fragments[0].inst.trim() === '' || fragments[0].inst.trim().split(' ')[0] === 'help') {

    const second = fragments.length > 0 && fragments[0].inst.trim().split(' ')[1]

    if (second) {
      for (var sink of [...sources, ...transformers, ...sinks])
        if (sink.mimes.filter(m => m === second).length > 0) {
          renderDoc(new sink.component({}))
        }
      return
    }

    console.log(`\nAvailable source adapters:\n`)
    for (var src of sources) {
      displaySimple(new src.component({}), src.mimes, chalk.blueBright)
    }

    console.log(`\nAvailable transformers:\n`)
    for (var tr of transformers)
      displaySimple(new tr.component({}), tr.mimes, chalk.yellowBright)

    console.log(`\nAvalaible sinks:\n`)
    for (var sink of sinks) {
      displaySimple(new sink.component({}), sink.mimes, chalk.redBright)
    }
    return
  }

  fragments.push({
    type: 'sink',
    inst: 'debug'
  })

  const pipe = await build_pipeline(fragments)
  const stream = instantiate_pipeline(pipe)
  do {
    var res = await stream.next()
  } while (res)
}

run().catch(e => console.error(e.stack))