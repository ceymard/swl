export * from './adapters'
import {Source, PipelineComponent} from './adapters'

if (process.env.DEBUG) require('source-map-support').install()

function try_require(...names: string[]) {
  for (var name of names) try { require(name) } catch { }
}

try_require(
  'swl.json',
  'swl.csv',
  'swl.yaml',
  'swl.postgres',
  'swl.sqlite',
  'swl.imap',
  'swl.mysql',
  'swl.oracle'
)

export const sources: {[name: string]: (str: string) => Source} = {}
export const sinks: {[name: string]: (str: string) => PipelineComponent} = {}

export function register_source(maker: (str: string) => Source, ...mimes: string[]) {
  for (var name of mimes)
    sources[name] = maker
}

export function register_sink(maker: (str: string) => PipelineComponent, ...mimes: string[]) {
  for (var name of mimes)
    sinks[name] = maker
}
