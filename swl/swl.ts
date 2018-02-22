#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'
import {PARSER, pipeline, sources, sinks, ADAPTER_AND_OPTIONS} from './lib'
import * as pth from 'path'

const args = process.argv.slice(2).join(' ')
// console.log(args)

async function run() {
  const fragments = PARSER.tryParse(args)
  // console.log(fragments)
  const pipe = []

  if (fragments.length < 1) {
    console.log(`Available source adapters:`)
    for (var x in sources) {
      if (x.indexOf('.') === 0 || x.indexOf('/') > -1) continue
      console.log(`  - ${x}`)
    }

    console.log(`Avalaible adapters:`)
    for (var x in sinks) {
      if (x.indexOf('.') === 0 || x.indexOf('/') > -1) continue
      console.log(`  - ${x}`)
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
    var handler = f.type === 'source' ? sources[name] : sinks[name]

    if (!handler) {
      var c = pth.parse(name)
      handler = f.type === 'source' ? sources[c.ext] : sinks[c.ext]
      rest = name + ' ' + rest
      // console.log(name)
    }

    // console.log(name, handler)
    // Check that handler exists !
    pipe.push(await handler(opts || {}, rest))

  }
  await pipeline(pipe[0], ...pipe.slice(1))
}

run().catch(e => console.error(e.stack))