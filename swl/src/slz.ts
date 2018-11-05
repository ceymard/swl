
// By default, all serializers can return undefined if the value did not exist. ????
// var s = string().from('pouet') // 'pouet'
// var s = string().stringify().from({what: true}) // `{"what": true}`
// var s = string().from(undefined) // undefined
// var s = string('pouet').from(undefined) // 'pouet'
// var s = string('pouet').from(null) // 'pouet'
// var s = string().nullable().from(null) // null
// var s = string(null).from(null) // null
// var s = string(null).from('pouet') // 'pouet' as string | null

// How do I handle absent values ?
// How do I handle the fact that a field may be computed from other fields from the source ?
// I need the serializer to be complete.


/**
 * A very basic cloning function
 * @param base the base object
 * @param newprops new properties to add to the object
 */
export function clone<T>(base: T, newprops: {[K in keyof T]?: T[K]}) {
  var res = Object.create(base.constructor)
  for (var x in base) {
    res[x] = base[x]
  }
  for (x in newprops) {
    res[x] = newprops[x]
  }
  return res
}


export class Serializer<T> {

  public _help: string = ''

  // Just to be used as typeof s.TYPE
  public TYPE!: T

  from(unk: unknown): T {
    return unk as T
  }

  default(def: T): Serializer<NonNullable<T>>
  default(def: null): Serializer<NonNullable<T> | null>
  default(def: T | null) {
    return this.transform((ser, v) => {
      var res = ser.from(v)
      return res !== undefined ? res : def
    })
  }

  required(): Serializer<NonNullable<T>> {
    return this.transform((ser, v) => {
      var res = ser.from(v)
      if (res == undefined)
        throw new Error(`this reader requires a value`)
      return res as NonNullable<T>
    })
  }

  or<U>(spec: Serializer<U>): Serializer<T | U> {
    return this.transform((ser, v) => {
      try {
        return ser.from(v)
      } catch {
        return spec.from(v)
      }
    })
  }

  when<K extends keyof T, V extends T[K], U>(key: K, value: V, ser: Serializer<U>) {

  }

  transform<U>(fn: (v: Serializer<T>, value: unknown) => U): Serializer<U> {
    return new TransformSerializer(this, fn)
  }

  help(): string
  help(tpl: string): this
  help(tpl: TemplateStringsArray): this
  help(tpl?: TemplateStringsArray | string) {
    if (typeof tpl === 'undefined')
      return this._help
    return clone(this as Serializer<T>, {_help: typeof tpl === 'string' ? tpl : tpl[0]})
  }

}


export class TransformSerializer<T, U> extends Serializer<U> {
  constructor(public orig: Serializer<T>, public fn: (s: Serializer<T>, value: unknown) => U) {
    super()
    this._help = orig._help
  }

  from(v: unknown): U {
    return this.fn(this.orig, v)
  }
}



export type ObjectSerializerProps<T> = {[K in keyof T]: Serializer<T[K]>}


export class ObjectSerializer<T extends object> extends Serializer<T> {

  props<U>(props: ObjectSerializerProps<U>): ObjectSerializer<T & U> {

  }

  /**
   * Specify properties that are *optional* on the deserialized type.
   * They can have defaults and not be undefined in the end, but
   * this method exists for those types you want to deserialize to
   * that have optional properties.
   *
   * Optional properties on target type are the *only* ones that should
   * be present here. All the rest should go to props.
   *
   * @param props The properties that are to be tagged as optional
   */
  optional<U>(props: ObjectSerializerProps<U>): ObjectSerializer<T & {[k in keyof U]?: U[k]}> {

  }

  /**
   *
   * @param typ The class this deserializer should give an object of.
   * @param typcheck2 Give this parameter the exact same value as `typ`.
   *  It is unused at runtime but serves for more type verification with
   *  typescript, to ensure that there are no excess properties in the current
   *  object specification.
   */
  createAs<T extends object, U extends T, V extends U = U>(
    this: Serializer<U>,
    typ: new (...a: any[]) => T,
    typcheck2?: new (...a: any) => V
  ): ObjectSerializer<T> {
  // createAs<U extends T>(typ: new (...a: any[]) => U): ObjectSerializer<U> {
      return null!
  }

  index<V>(values: Serializer<V>): ObjectSerializer<{[n: string]: V}> {
    return null!
  }

  from(t: unknown): T {
    return t as T
  }

}


export class BooleanSerializer extends Serializer<boolean | undefined> {

  from(t: unknown) {
    return t !== undefined ? !!t : undefined
  }

}

export class StringSerializer extends Serializer<string | undefined> {
  from(t: unknown) {
    if (t == null) return undefined
    // FIXME check that t is indeed a string.
    return (t as any).toString() as string
  }

  serialize(t: string) {
    return t
  }
}

export class NumberSerializer extends Serializer<number | undefined> {

  from(t: unknown) {
    if (typeof t === 'number')
      return t
    if (typeof t === 'string') {
      return parseFloat(t) || parseInt(t, 16)
    }
    throw new Error('not a number')
  }

}


export function number(n: null): Serializer<number | null>
export function number(def: number): Serializer<number>
export function number(): Serializer<number | undefined>
export function number(def?: any): Serializer<any> {
  var res = new NumberSerializer()
  if (def !== undefined)
    return res.default(def)
  return res
}


export function string(): Serializer<string | undefined>
export function string(def: null): Serializer<string | null>
export function string(def: string): Serializer<string>
export function string(def?: string | null): Serializer<string | null | undefined> {
  var res = new StringSerializer()
  if (def !== undefined)
    return res.default(def!)
  return res
}


export function object(): ObjectSerializer<{}>
export function object<T extends object>(specs: ObjectSerializerProps<T>): ObjectSerializer<T>
export function object<T extends object>(specs: ObjectSerializerProps<T>, inst?: new (...a: any[]) => T): Serializer<T>
export function object<T extends object>(specs?: ObjectSerializerProps<T>, inst?: new (...a: any[]) => T): ObjectSerializer<any> {
  var res = new ObjectSerializer<T>()
  if (specs)
    res = res.props(specs)
  if (inst)
    res = res.createAs(inst)
  return res
}


export function indexed<T>(items: Serializer<T>): Serializer<{[name: string]: T}> {
  return null!
}


export type Unserializify<T> = T extends Serializer<infer U> ? U : T


export function tuple<Arr extends Serializer<any>[]>(...sers: Arr): Serializer<{[K in keyof Arr]: Unserializify<Arr[K]>} | undefined> {
  return null!
}


export function array<T>(items: Serializer<T>): Serializer<T[] | undefined> {
  return null!
}


export function boolean(): BooleanSerializer
export function boolean(def: boolean): Serializer<boolean>
export function boolean(def?: boolean) {
  var res = new BooleanSerializer()
  if (def !== undefined)
    return res.default(def)
  return res
}


