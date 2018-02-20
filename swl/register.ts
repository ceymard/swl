import {Source, PipelineComponent} from './adapters'

export type SourceMaker = (opts: any, str: string) => Promise<Source>
export type SinkMaker = (opts: any, str: string) => Promise<PipelineComponent>
export const sources: {[name: string]: SourceMaker} = {}
export const sinks: {[name: string]: SinkMaker} = {}

export function register_source(maker: SourceMaker, ...mimes: string[]) {
  for (var name of mimes)
    sources[name] = maker
}

export function register_sink(maker: SinkMaker, ...mimes: string[]) {
  for (var name of mimes)
    sinks[name] = maker
}

