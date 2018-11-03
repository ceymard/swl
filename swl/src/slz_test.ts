import * as s from './slz'


class Test {
  readonly a!: string | null
  readonly c!: number
}

class Test2 extends Test {
  d!: string
}

var a = s.object().props({
    a: s.string(null)
  })
var b = a.props({
  c: s.number(),
  // d: string()
})

// Ok
b.createAs(Test, Test)

var d = b.props({
  d: s.string('yes')
})

// Ok
d.createAs(Test2, Test2)
// Error : d is not defined on Test
d.createAs(Test, Test)


class NotOptional {
  a!: string
}

var opt = s.object().optional({a: s.string()})
// Error : Optional.a is not optional !
opt.createAs(NotOptional, NotOptional)
// Error : s.string() is undefined, but not Optional.a
s.object().props({a: s.string()}).createAs(NotOptional, NotOptional)
// OK : works with a default value
s.object().props({a: s.string('')}).createAs(NotOptional, NotOptional)

class Optional {
  a?: string
}

// Ok
s.object().optional({a: s.string()}).createAs(Optional, Optional)
// Error : property is required in the serializer
s.object().props({a: s.string()}).createAs(Optional, Optional)


class Optional2 {
  a?: string
  b!: number
}

// Ok
s.object().props({b: s.number(0)}).createAs(Optional2, Optional2)