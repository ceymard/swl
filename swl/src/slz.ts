

export class Serializer<T> {

  protected _default: T | undefined = undefined
  public _help: string

  serialize(arg: T): unknown {
    return arg
  }

  deserialize(unk: unknown): T {
    return unk as T
  }

  default(def: T) {
    this._default = def
    return this as any as Serializer<NonNullable<T>>
  }

  help(tpl: TemplateStringsArray) {
    this._help = tpl[0]
    return this
  }

}


export type ObjectSerializerProps<T> = {[K in keyof T]: Serializer<T[K]>}


export class ObjectSerializer<T extends object> extends Serializer<T> {

  constructor(
    public specs: ObjectSerializerProps<T>,
    public inst_type?: new (...a: any[]) => T
  ) {
    super()
  }

  deserialize(t: unknown): T {
    return t as T
  }

  serialize(t: T) {
    return t
  }

}


export class BooleanSerializer extends Serializer<boolean> {

  deserialize(t: unknown) {
    return !!t
  }

  serialize(t: boolean) {
    return !!t
  }

}

export class StringSerializer extends Serializer<string> {
  deserialize(t: unknown) {
    // FIXME check that t is indeed a string.
    return t as string
  }

  serialize(t: string) {
    return t
  }
}

export function string(): Serializer<string>
export function string(def: string): Serializer<string>
export function string(def?: string): Serializer<string> {
  var res = new StringSerializer()
  if (def !== undefined)
    return res.default(def)
  return res
}


export function object<T extends object>(specs: ObjectSerializerProps<T>, inst?: new (...a: any[]) => T) {
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


export type BaseType<T> = T extends Serializer<infer U> ? U : T