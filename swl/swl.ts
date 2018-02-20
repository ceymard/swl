#!/usr/bin/env node

// import {PARSER} from './cmdparse'
// import {PARSER as OPARSER} from './oparse'
// import {Adapter, registry} from './adapters'
import {PARSER, pipeline, sources, sinks, ADAPTER_AND_OPTIONS} from './lib'

// console.log(URI_WITH_OPTS.tryParse(`./myfile.csv?zob.cs `))


const args = `
  inline-json?col1 {"a": 1, "b": 2}, {"a": 5, "b": 6}
  |< inline-json?col2 {"a": "zobi", "b": "zob"}
  | debug
`

async function run() {
  const fragments = PARSER.tryParse(args)
  const pipe = []
  for (var f of fragments) {
    const [name, opts, rest] = ADAPTER_AND_OPTIONS.tryParse(f.inst)
    const handler = f.type === 'source' ? sources[name] : sinks[name]

    // Check that handler exists !
    pipe.push(await handler(opts, rest))

  }
  pipeline(pipe[0], ...pipe.slice(1))
}

run().catch(e => console.error(e.stack))