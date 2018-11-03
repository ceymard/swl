
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

  public def: T | undefined | null = undefined
  public _help: string = ''

  // Just to be used as typeof s.TYPE
  get TYPE(): T { return null! }

  from(unk: unknown): T {
    return unk as T
  }


  default(def: T): Serializer<NonNullable<T>>
  default(def: null): Serializer<NonNullable<T> | null>
  default(def: T | null) {
    var res = clone(this as Serializer<T>, {def})
    if (def == null) {
      return res.nullable()
    }
    return res
  }

  nullable(): Serializer<T | null> {

  }

  or<U>(spec: Serializer<U>): Serializer<T | U> {

  }

  when<K extends keyof T>(key: K, value: T[K], ) {

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


export type ObjectSerializerProps<T> = {[K in keyof T]: Serializer<T[K]>}


export class ObjectSerializer<T extends object> extends Serializer<T> {

  constructor(
    public specs?: ObjectSerializerProps<T>,
    public inst_type?: new (...a: any[]) => T
  ) {
    super()
  }

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
  createAs<T extends object, U extends Helper<T, keyof T>, V extends U = U>(
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


export class BooleanSerializer extends Serializer<boolean> {

  from(t: unknown) {
    return !!t || !!this.def
  }

  serialize(t: boolean) {
    return !!t
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


export class OrNull<T> extends Serializer<T | null> {

}


export function number(): Serializer<number>
export function number(): Serializer<number> {

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
export function object<T extends object>(specs?: ObjectSerializerProps<T>, inst?: new (...a: any[]) => T) {
  return new ObjectSerializer<T>(specs, inst)
}

export function boolean(): BooleanSerializer
export function boolean(def: boolean): Serializer<boolean>
export function boolean(def?: boolean) {
  var res = new BooleanSerializer()
  if (def !== undefined)
    return res.default(def)
  return res
}



namespace Test {

  class Test {
    readonly a!: string | null
    readonly c!: number
  }

  class Test2 extends Test {
    d!: string
  }

  var a = object().optional({
      a: string(null)
    })
  var b = a.props({
    c: number(),
    // d: string()
  })
  var c = b.createAs(Test, Test)

  var d = c.props({
    d: string()
  })

  var e = d.createAs(Test2, Test2)

}
