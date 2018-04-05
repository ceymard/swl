#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'
import {PARSER, sources, sinks, ADAPTER_AND_OPTIONS, build_pipeline, FactoryObject} from './lib'
import { readFileSync } from 'fs'

const args = process.argv.slice(2)
// console.log(args)

function displaySimple(obj: FactoryObject) {
  console.log(`  - ${obj.mimes[0]}`)
}

async function run() {
  const contents = args[0] !== '-f' ? args.join(' ') : readFileSync(args[1], 'utf-8').replace(/^\s*#[^\n]*$/gm, '')
  // console.log(contents)
  const fragments = PARSER.tryParse(contents)
  // console.log(fragments)
  const pipe = []

  if (fragments.length < 1 || fragments[0].inst.trim() === '') {
    console.log(`Available source adapters:`)
    for (var src of sources) {
      displaySimple(src)
    }

    console.log(`Avalaible sinks:`)
    for (var x in sinks.map) {
      if (x.indexOf('.') === 0 || x.indexOf('/') > -1) continue
      const obj = sinks.map[x]!
      displaySimple(obj)
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