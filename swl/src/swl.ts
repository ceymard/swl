#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'
import {FRAGMENTS, sources, sinks, instantiate_pipeline, FactoryObject, yup, build_pipeline} from './lib'
// import { readFileSync } from 'fs'
import chalk from 'chalk'

// console.log(process.argv)
const args = process.argv.slice(2)
// console.log(args)

function renderDoc(obj: FactoryObject) {
  const marked = require('marked')
  const TerminalRenderer = require('marked-terminal')

  marked.setOptions({
    renderer: new TerminalRenderer()
  })

  const schema = obj.schema as yup.ObjectSchema<any>
  console.log(schema.describe())
  console.log(marked(obj.help))
}

function displaySimple(obj: FactoryObject, color: ((s: string) => string)) {
  const first = obj.help.split('\n').map(l => l.trim()).filter(id => id)[0]
  console.log(`  - ${color(obj.mimes[0])}${obj.mimes.length > 1 ? ' ' : ''}${chalk.gray(obj.mimes.slice(1).join(', '))} ${first}`)
  // renderDoc(obj)
}

async function run() {

  const contents = args
  .map(a => a.indexOf(' ') > -1 ? `'${a.replace(/'/g, "\\'")}'` : a)
  .join(' ')

  const fragments = FRAGMENTS.tryParse(contents)

  if (fragments.length < 1 || fragments[0].inst.trim() === '' || fragments[0].inst.trim().split(' ')[0] === 'help') {

    const second = fragments.length > 0 && fragments[0].inst.trim().split(' ')[1]

    if (second) {
      for (var sink of [...sources, ...sinks])
        if (sink.mimes.filter(m => m === second).length > 0)
          renderDoc(sink)
      return
    }
    // console.log(second)

    console.log(`\nAvailable source adapters:\n`)
    for (var src of sources) {
      displaySimple(src, chalk.blueBright)
    }

    console.log(`\nAvalaible sinks:\n`)
    for (var sink of sinks) {
      displaySimple(sink, chalk.redBright)
    }
    return
  }

  fragments.push({
    type: 'sink',
    inst: 'debug'
  })

  const pipe = await build_pipeline(fragments)
  const pipeline = instantiate_pipeline(pipe)
  do {
    var res = await pipeline.next()
  } while (!res.done)
}

run().catch(e => console.error(e.stack))