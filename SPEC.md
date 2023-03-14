# Recap Type Spec

* Version 0.1.0

## Introduction

This document defines Recap's types. It is intended to be the authoritative specification. Recap type implementations must adhere to this document.

This spec uses [YAML](https://yaml.org) to provide examples, but Recap's types are agnostic to the serialized format. Recap types may be defined in YAML, TOML, JSON, or any other compatible language.

## Types

Recap supports the following types:

* `null`
* `bool`
* `int`
* `float`
* `string`
* `bytes`
* `list`
* `map`
* `struct`
* `enum`
* `union`

Note: These types are inspired by [Apache Arrow's Schema.fbs](https://github.com/apache/arrow/blob/main/format/Schema.fbs).

### `null`

A null value.

### `bool`

A boolean value.

### `int`

An integer value with a fixed bit length.

#### Attributes

* `bits`: Number of bits. (type: int32, required: true)
* `signed`: False means the integer is unsigned. (type: bool, required: false, default: true)

#### Examples

```yaml
# A 32-bit signed integer
type: int
bits: 32
signed: true
```

### `float`

An IEEE 754 encoded floating point value with a fixed bit length.

#### Attributes

* `bits`: Number of bits. (type: int32, required: true)

#### Examples

```yaml
# A 32-bit IEEE 754 encoded float
type: float
bits: 32
```

### `string`

A UTF-8 encoded Unicode string with a maximum byte length.

#### Attributes

* `bytes`: The maximum number of bytes to store the string. (type: int32, required: true)
* `variable`: If true, the string is variable-length (`<= bytes`). (type: bool, required: false, default: true)

#### Examples

```yaml
# A VARCHAR(255)
type: string
bytes: 255
variable: true
```

### `bytes`

A byte array value.

#### Attributes

* `bytes`: The maximum number of bytes that can be stored in the byte array. (type: bool, required: true)
* `variable`: If true, the byte array is variable-length (up to the byte max). (type: bool, required: false, default: true)

#### Examples

```yaml
# A VARBINARY(255)
type: bytes
bytes: 255
variable: true
```

### `list`

A list of values all sharing the same type.

#### Attributes

* `values`: (type: `type`, required: true)
* `bits`: (type: bool | null, required: false, default: null)

#### Examples

```yaml
# A list of unsigned 64-bit integers
type: list
values:
    type: int
    bits: 64
    signed: false
```

### `map`

A map of key/value pairs where each key is the same type and each value is the same type.

#### Attributes

* `keys`: Type for all items in the key set. (type: `type`, required: true)
* `values`: Type for all value items. (type: `type`, required: true)

#### Examples

```yaml
# A map from 32-bit strings to boolean values
type: map
keys:
    type: string
    bytes: 2_147_483_647
values:
    type: bool
```

### `struct`

A list of fields.

#### `struct` Attributes

* `fields`: (type: `list[type]`, required: true, default: [])

#### `field` Attributes

A field can be any recap type. Fields have the following additional attributes:

* `name`: The field's name. (type: string32 | null, required: false, default: null)
* `default`: The default value for a reader if the field is not set in the struct. (type: literal, required: false)
* `nullable`: If false, the field's value must be set. `nullable=true` is syntactic sugar that wraps the field's type in a union of `types=[null, type]` with a `null` default (if default is not explicitly defined). (type: bool, required: false, default: true)

#### Examples

```yaml
# A struct with a required signed 32-bit integer field called "id"
type: struct
fields:
    - name: id
      type: int
      bits: 32
      nullable: false
```

### `enum`

An enumeration of string symbols.

#### Attributes

* `symbols`: An ordered list of string symbols. (type: `list[string]`, required: true)

#### Examples

```yaml
# An enum with RGB symbols
type: enum
symbols: ["RED", "GREEN", "BLUE"]
```

### `union`

A value that can be one of several types. It is acceptable for a value to be more than one of the types in the union.

#### Attributes

* `types`: A list of types the value can be. (type: `list[type]`, required: true)

#### Examples

```yaml
# A union type of null or a 32-bit signed int
type: union
types:
    - type: null
    - type: int
      bits: 32
```

## Documentation

All types support a `doc` attribute, which allows developers to document types.

* `doc`: A documentation string for the type. (type: string32 | null, required: false, default: null)

```yaml
type: union
doc: A union type of null or a 32-bit signed int
types:
    - type: null
    - type: int
      bits: 32
```

## Aliases

All types support an `alias` attribute. `alias` allows a type to reference previously defined types.

* `alias`: An alias for the type. (type: string32 | null, required: false, default: null)

### Built-in Aliases

Recap includes the following built-in type aliases:

* `int8`
* `uint8`
* `int16`
* `uint16`
* `int32`
* `uint32`
* `int64`
* `uint64`
* `float16`
* `float32`
* `float64`
* `string32`
* `string64`
* `bytes32`
* `bytes64`
* `uuid`
* `decimal`
* `decimal128`
* `decimal256`
* `duration64`
* `interval128`
* `time32`
* `time64`
* `timestamp64`
* `date32`
* `date64`

#### `int8`

##### Definition

```yaml
type: int
alias: int8
bits: 8
```

#### `uint8`

##### Definition

```yaml
type: int
alias: uint8
bits: 8
signed: false
```

#### `int16`

##### Definition

```yaml
type: int
alias: int16
bits: 16
```

#### `uint16`

##### Definition

```yaml
type: int
alias: uint16
bits: 16
signed: false
```

#### `int32`

##### Definition

```yaml
type: int
alias: int32
bits: 32
```

#### `uint32`

##### Definition

```yaml
type: int
alias: uint32
bits: 32
signed: false
```

#### `int64`

##### Definition

```yaml
type: int
alias: int64
bits: 64
```

#### `uint64`

##### Definition

```yaml
type: int
alias: uint64
bits: 64
signed: false
```

#### `float16`

##### Definition

```yaml
type: float
alias: float16
bits: 16
```

#### `float32`

##### Definition

```yaml
type: float
alias: float32
bits: 32
```

#### `float64`

##### Definition

```yaml
type: float
alias: float64
bits: 64
```

#### `bytes32`

##### Definition

```yaml
type: bytes
alias: bytes32
bytes: 2_147_483_647
```

#### `bytes64`

##### Definition

```yaml
type: bytes
alias: bytes64
bytes: 9_223_372_036_854_775_807
```

#### `uuid`

A UUID in 8-4-4-4-12 string format (XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX) as defined in [RFC 4122](https://www.ietf.org/rfc/rfc4122.txt). 

##### Definition

```yaml
type: string
alias: uuid
bytes: 36
variable: false
```

#### `decimal`

An arbitrary-precision decimal number. This type is the same as [Avro's Decimal](https://avro.apache.org/docs/1.10.2/spec.html#Decimal).

##### Attributes

* `precision`: Total number of digits. 123.456 has a precision of 6. (type: int, required: true)
* `scale`: Digits to the right of the decimal point. 123.456 has a scale of 3. (type: int, required: true)

##### Definition

```yaml
type: bytes32
alias: decimal
```

#### `decimal128`

An arbitrary-precision decimal number stored in a fixed length 128-bit byte array. This type is the same as [Arrow's decimal128](https://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html#pyarrow.decimal128).

##### Attributes

* `precision`: Total number of digits. 123.456 has a precision of 6. (type: int, required: true)
* `scale`: Digits to the right of the decimal point. 123.456 has a scale of 3. (type: int, required: true)

##### Definition

```yaml
type: bytes
alias: decimal128
bytes: 16
variable: false
```

#### `decimal256`

An arbitrary-precision decimal number stored in a fixed length 256-bit byte array. This type is the same as [Arrow's decimal256](https://arrow.apache.org/docs/r/reference/data-type.html).

##### Attributes

* `precision`: Total number of digits. 123.456 has a precision of 6. (type: int, required: true)
* `scale`: Digits to the right of the decimal point. 123.456 has a scale of 3. (type: int, required: true)

##### Definition

```yaml
type: bytes
alias: decimal256
bytes: 32
variable: false
```

#### `duration64`

A length of time without timezones and leap seconds. This type is the same as [Arrow's Duration](https://arrow.apache.org/docs/python/generated/pyarrow.duration.html) but with a superset of time units.

##### Attributes

* `unit`: A string time unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND (type: string32, required: true)

##### Definition

```yaml
type: int64
alias: duration64
```

#### `interval128`

An interval of time on a calendar. This measurement allows you to measure time without worrying about leap seconds, leap years, and time changes. Years, quarters, hours, and minutes can be expressed using this type.

The interval is measured in months, days, and an intra-day time measurement. Months and days are each 32-bit signed integers. The remainder is a 64-bit signed integer measured in a certain time unit. Leap seconds are ignored.

This type is the same as [Arrow's month_day_nano_interval](https://arrow.apache.org/docs/python/generated/pyarrow.month_day_nano_interval.html) but with a superset of time units.

##### Attributes

* `unit`: A string time unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND (type: string32, required: true)

##### Definition

```yaml
type: bytes
alias: interval128
bytes: 16
variable: false
```

#### `time32`

Time since midnight without timezones and leap seconds in a 32-bit integer. This type is the same as [Arrow's time32](https://arrow.apache.org/docs/python/generated/pyarrow.time32.html) but with a superset of time units.

##### Attributes

* `unit`: A string time unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND (type: string32, required: true)

##### Definition

```yaml
type: int32
alias: time32
```

#### `time64`

Time since midnight without timezones and leap seconds in a 64-bit integer. This type is the same as [Arrow's time64](https://arrow.apache.org/docs/python/generated/pyarrow.time64.html) but with a superset of time units.

##### Attributes

* `unit`: A string time unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND (type: string32, required: true)

##### Definition

```yaml
type: int64
alias: time64
```

#### `timestamp64`

Time elapsed since a specific epoch.

A timestamp with no timezone is a `datetime` in database parlance--a date and time as you would see it on wrist-watch.

A timestamp with a timezone represents the amount of time elapsed since the 1970-01-01 00:00:00 epoch in UTC time zone (regardless of the timezone that's specified). Readers must translate the UTC timestamp to a timestamp value for the specified timezone. See Apache Arrow's [Schema.fbs](https://github.com/apache/arrow/blob/main/format/Schema.fbs) documentation for more details.

This type is the same as [Arrow's timestamp](https://arrow.apache.org/docs/python/generated/pyarrow.timestamp.html) but with a superset of time units.

##### Attributes

* `unit`: A string time unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND (type: string32, required: true)
* `timezone`: An optional `Olson timezone database` string. (type: string32 | null, required: false, default: null)

##### Definition

```yaml
type: int64
alias: timetamp64
```

#### `date32`

Date since the UNIX epoch without timezones and leap seconds in a 32-bit integer. This type is the same as [Arrow's date32](https://arrow.apache.org/docs/python/generated/pyarrow.date32.html) but with a superset of time units.

##### Attributes

* `unit`: A string time unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND (type: string32, required: true)

##### Definition

```yaml
type: int32
alias: date32
```

#### `date64`

Date since the UNIX epoch without timezones and leap seconds in a 64-bit integer. This type is the same as [Arrow's date64](https://arrow.apache.org/docs/python/generated/pyarrow.date64.html) but with a superset of time units.

##### Attributes

* `unit`: A string time unit: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND (type: string32, required: true)

##### Definition

```yaml
type: int64
alias: date64
```

### References

Aliases are referenced using the `type` field:

```yaml
type: struct
doc: A chapter in a book
fields:
    - name: previous
      alias: com.mycorp.models.Page
      type: int
      bits: 32
      signed: false
    - name: next
      type: com.mycorp.models.Page
```

In this example, `next`'s type will be the same as `previous`'s.

Recap also allows cyclic references:

```yaml
alias: com.mycorp.models.LinkedListUint32
type: struct
doc: A linked list of unsigned 32-bit integers
fields:
    - name: value
      type: int
      bits: 32
      signed: false
    - name: next
      type: com.mycorp.models.LinkedListUint32
```

### Additional Attributes

Aliases may define additional required or optional attributes.

### Logical Types

Aliases may carry semantic meaning. For example, a UTC `timestamp64` is defined as:

```yaml
type: int64
timezone: UTC
```

But without the `type` set to `timestamp64`, the `timezone` attribute will be ignored and the type will be treated as a normal 64-bit signed integer.

### Nested References

Type aliases can extend other type aliases, too:

```yaml
type: com.mycorp.models.ParentType
alias: com.mycorp.models.NestedType
color: blue
```

Nested types with attributes will overwrite any defined attributes with the same name in the parent type. In this example, `NestedType`'s `color: blue` overwrites any `color` defined in `ParentType`.

### Namespaces

Aliases are globally unique, so they must include a unique dotted namespace prefix. Naked aliases (aliases with no dotted namespace) are reserved for Recap.

## FAQ

### Is this a serialization format?

No. Recap does not define how data is persisted to disk. It's meant to be more of a translation layer to work with data records without worrying whether they came from Avro, JSON, Protobuf, Parquet, or a database.

### May I define extra type attributes?

Yes. Recap will ignore them.

### Why did you include enums?

Recap's type system is constrained enough that enums can't be expressed as type unions anymore. In a more expressive type system you could say an enum was a union of constant strings (literals). That's out the window since Recap doesn't have a literal.

DISCUSS: Between default values and enums, I wonder if I should just include literals instead.

### Why didn't you include constraints?

I plan to add constraints to Recap as a separate add-on spec (similar to [JSON schema's validators](https://json-schema.org/draft/2020-12/json-schema-validation.html), [protoc-gen-validate](https://github.com/bufbuild/protoc-gen-validate), and [JSR 380](https://beanvalidation.org/2.0-jsr380/)). The constraints will be *very* modest.

I made this decision because I found mixing constraints and types to be difficult. Defining an `int` that's >= 0 and <= 4_294_967_296 is the same range as a uint32, but doesn't necessarily say anything about the physical format. When you start adding in floats, IEEE standards, and arbitrary precision decimals, these distinctions are important. While they could be encoded in a pure type system, I felt making them explicit--including `bits` as an attribute, for example--was more pragmatic. It also aligned with Arrow's `Schema.fbs` does.

Once I committed to keeping constraints separate from types, it felt more natural to keep constraints out of the type spec all together.

### Why didn't you limit integer and float bits to specific values (16, 32, 64, and so on)?

I couldn't find a good reason to. The common bit lengths are implemented as aliases.

### Why is it called a `struct`?

Struct, record, schema, and object are all common. A "schema" containing a "schema" is strange and "objects" usually have methods. "record" could have worked, but seemed less well known. Thus, I went with "struct".

### Why didn't you use a more expressive type system like CUE?

Frankly, I'm not an academic and I don't have enough specialty in type systems to contribute to that area. I did spend time with CUE (and a little with KCL), and found them to be unwieldy. I didn't want templating, and I only want to include a small set of constraints in Recap. Elaborate type systems and templating felt like overkill.

Kafka Connect is a schema example that is much more constrained than something like CUE, while still modeling most of what Recap wants to model. Kafka Connect has [hundreds of source and sink connectors](https://docs.confluent.io/cloud/current/connectors/index.html#preview-connectors).

[Apache Arrow](https://arrow.apache.org) is in the same vein as Kafka Connect. In fact, Recap's base types and many of its `alias`es are taken directly from Apache Arrow. Again, Arrow has proven to work with dozens of different databases and data frameworks.

### So, why didn't you use Apache Arrow?

For starters, Arrow doesn't have a schema definition language; it's entirely programmatic. Their spec is their [Schema.fbs](https://github.com/apache/arrow/blob/main/format/Schema.fbs). You'd have to write code to define a schema; this isn't what I wanted.

In addition, their in-memory model is more optimized for columnar analytical work. Recap's main focus is on record-based data from APIs, message buses, and databases. Single record operations and schema manipulation tend to be different than analytical operations. Counting and grouping doesn't make much sense on a single record. Schema manipulation is where Recap shines: projecting out fields, merging and intersecting schemas, and checking for schema compatibility.

### How do you handle coercion?

Coercing types that don't fit cleanly into Recap's type system is outside the scope of this spec. Recap will include a test suite that defines how specific types are meant to be converted to and from Recap types on a per-SDL basis. For example, converting PostgreSQL's `geography(point)` type to/from a Recap type is something that will be defined in the test suite and documented in the PostgreSQL converter documentation.
