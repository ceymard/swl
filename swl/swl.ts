#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'
import {PARSER, pipeline, sources, sinks, ADAPTER_AND_OPTIONS} from './lib'

const args = process.argv.slice(2).join(' ')
// console.log(args)

async function run() {
  const fragments = PARSER.tryParse(args)
  // console.log(fragments)
  const pipe = []

  if (fragments.length < 2) {
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

  for (var f of fragments) {
    const [name, opts, rest] = ADAPTER_AND_OPTIONS.tryParse(f.inst)
    const handler = f.type === 'source' ? sources[name] : sinks[name]

    // console.log(name, handler)
    // Check that handler exists !
    pipe.push(await handler(opts || {}, rest))

  }
  await pipeline(pipe[0], ...pipe.slice(1))
}

run().catch(e => console.error(e.stack))