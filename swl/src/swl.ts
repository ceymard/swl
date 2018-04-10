#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'
import {PARSER, sources, sinks, ADAPTER_AND_OPTIONS, build_pipeline, FactoryObject, yup} from './lib'
import { readFileSync } from 'fs'
import chalk from 'chalk'

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
  const contents = args[0] !== '-f' ? args
    .map(a => a.indexOf(' ') > -1 ? `'${a.replace("'", "\\'")}'` : a)
    .join(' ') : readFileSync(args[1], 'utf-8').replace(/^\s*#[^\n]*$/gm, '')
  // console.log(contents)
  const fragments = PARSER.tryParse(contents)
  // console.log(fragments)
  const pipe = []
  if (fragments.length < 1 || fragments[0].inst.trim() === '' || fragments[0].inst.trim().split(' ')[0] === 'help') {

    const second = fragments[0].inst.trim().split(' ')[1]

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
  // if (fragments.length === 1) {
  // Maybe should check if there is not a debug already present
  fragments.push({
    type: 'sink',
    inst: 'debug'
  })
  // }

  for (var f of fragments) {
    var [name, opts, rest] = ADAPTER_AND_OPTIONS.tryParse(f.inst)
    var handler = f.type === 'source' ?
      await sources.get(name, opts, rest) :
      await sinks.get(name, opts, rest)
    pipe.push(handler)

    // console.log(name, handler)
    // Check that handler exists !
    // pipe.push(await handler(opts || {}, rest))

  }

  const pipeline = build_pipeline(pipe)
  do {
    var res = await pipeline.next()
  } while (!res.done)
  // await pipeline(pipe[0], ...pipe.slice(1))
}

run().catch(e => console.error(e.stack))