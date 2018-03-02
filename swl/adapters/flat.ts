import { flatten as flt, unflatten as unflt } from 'flat'
import { simple_transformer } from './adapter'

simple_transformer(function flatten(opts: any, a: any) {
  return flt(a, opts)
}, 'flatten')

simple_transformer(function unflatten(opts: any, a: any) {
  return unflt(a, opts)
}, 'unflatten')
