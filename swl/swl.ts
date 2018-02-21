#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'
import {PARSER, pipeline, sources, sinks, ADAPTER_AND_OPTIONS} from './lib'

// console.log(URI_WITH_OPTS.tryParse(`./myfile.csv?zob.cs `))


// const args = `
//   inline-json?col1 {"a": 1, "b": 2}, {"a": 5, "b": 6}
//   -: inline-json?col2 {"a": "zobi", "b": "zob"}
//   -: sqlite test.db
//   :: sanitize
//   :: csv test-*.csv?encoding:latin1
// `

const args = process.argv.slice(2).join(' ')
// console.log(args)

async function run() {
  const fragments = PARSER.tryParse(args)
  // console.log(fragments)
  const pipe = []
  if (fragments.length < 2) throw new Error(`Need a pipeline`)

  for (var f of fragments) {
    // console.log(f)
    const [name, opts, rest] = ADAPTER_AND_OPTIONS.tryParse(f.inst)
    const handler = f.type === 'source' ? sources[name] : sinks[name]

    // console.log(name, handler)
    // Check that handler exists !
    pipe.push(await handler(opts || {}, rest))

  }
  await pipeline(pipe[0], ...pipe.slice(1))
}

run().catch(e => console.error(e.stack))