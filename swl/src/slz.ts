

export class Serializer<T> {

  protected _default: T | undefined = undefined

  serialize(arg: T): unknown {
    return null
  }

  deserialize(unk: unknown): T {
    return null!
  }

  default(def: T) {
    this._default = def
    return this as any as Serializer<NonNullable<T>>
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
    return null!
  }

  serialize(t: T) {

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