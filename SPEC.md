# Recap Type Spec

* Version 0.1.0

## Introduction

This document defines Recap's types. It is intended to be the authoritative specification. Recap type implementations must adhere to this document.

This spec uses [YAML](https://yaml.org) to provide examples, but Recap's types are agnostic to the serialized format. Recap types may be defined in YAML, TOML, JSON, or any other compatible language.

### What is Recap's Type Spec?

Recap's type spec describes a data model that can model relation database schemas and RPC IDLs with minimal type coercion. It's similar to [Apache Arrow](https://arrow.apache.org/)'s [Schema.fbs](https://github.com/apache/arrow/blob/main/format/Schema.fbs) or [Apache Kafka](https://kafka.apache.org)'s [Schema.java](https://github.com/apache/kafka/blob/trunk/connect/api/src/main/java/org/apache/kafka/connect/data/Schema.java).

### Why Does Recap Type Spec Exist?

Data passes through web services, databases, message brokers, and object stores. Each system describes its data differently. Developers have historically written schema conversion logic and tooling for each system. Building custom logic and tooling for each system is inefficient and error-prone. Recap's type system describes these schemas in a standard data model so a single set of tools can be built to deal with the data.

[Recap](https://github.com/recap-cloud/recap) uses this type system to:

* Track and compare schemas: `log` and `diff` schemas.
* Check schema compatibility: Validate that schemas are backward, forward, or fully compatible.
* Transform schemas: `intersect`, `union`, and `project` schemas.
* Transpile schemas: Convert between various IDLs and database DDLs.

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

This section defines each type.

*NOTE: Attribute types in the spec define what type a Recap implementation should use when storing the attribute. Attribute types are not the same as Recap types unless noted.*

### `null`

A null value.

### `bool`

A boolean value.

### `int`

An integer value with a fixed bit length.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| bits | Number of bits. | 32-bit signed integer | YES | |
| signed | False means the integer is unsigned. | Boolean | NO | true |

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

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| bits | Number of bits. | 32-bit signed integer | YES | |

#### Examples

```yaml
# A 32-bit IEEE 754 encoded float
type: float
bits: 32
```

### `string`

A UTF-8 encoded Unicode string with a maximum byte length.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| bytes | The maximum number of bytes to store the string. | 64-bit signed integer | YES | |
| variable | If true, the string is variable-length (`<= bytes`). | Boolean | NO | true |

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

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| bytes | The maximum number of bytes that can be stored in the byte array. | 64-bit signed integer | YES | |
| variable | If true, the byte array is variable-length (`<= bytes`). | Boolean | NO | true |

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

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| values | Type for all items in the list. | *Recap type object* | YES | |
| length | The maximum length of the list. If unset, the list size is infinite. | 64-bit signed integer \| null | NO | null |
| variable | If true, the list is variable-length (`<= length` if `length` is set). If false, `length` must be set. | Boolean | NO | true |


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

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| keys | Type for all items in the key set. | *Recap type object* | YES | |
| values | Type for all value items. | *Recap type object* | YES | |


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

An ordered collection of Recap types. Table schemas are typically represented as Recap structs, though the two are not the same. Recap structs support additional features like the nested structs (like a [Protobuf Message](https://protobuf.dev/reference/protobuf/proto3-spec/#message_definition)) and unnamed fields (like a CSV file with no header).

#### `struct` Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| fields | An ordered list of Recap types. | *List of Recap type objects* | NO | [] |

#### `field` Attributes

Recap types in the `fields` attribute can have two extra attributes set: `name` and `default`.

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| name | The field's name. | String \| null | NO | null |
| default | The default value for a reader if the field is not set in the struct. | Literal of any type | NO | |

An unset default is differentiated from a default with a null value. An unset default is treated as "no default", while a default that's been set to null is treated as a null default.

*NOTE: Database defaults often appear as strings like `nextval('\"public\".some_id_seq'::regclass)` or `'USD'::character varying`. Such defaults are left to the developer to interpret based on the database they're using.*

#### Examples

```yaml
# A struct with a required signed 32-bit integer field called "id"
type: struct
fields:
  - name: id
    type: int
    bits: 32
  - name: email
    type: string
    bytes: 255
```

Optional fields are expressed as a union with a `null` type and a null default (similar to [Avro's fields](https://avro.apache.org/docs/1.10.2/spec.html#schema_record)).

```yaml
# A struct with an optional string field called "secondary_phone"
type: struct
fields:
  - name: secondary_phone
    type: union
    types: ["null", "string32"]
    default: null
```

### `enum`

An enumeration of string symbols.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| symbols | An ordered list of string symbols. | List of strings | YES | |

#### Examples

```yaml
# An enum with RGB symbols
type: enum
symbols: ["RED", "GREEN", "BLUE"]
```

### `union`

A value that can be one of several types. It is acceptable for a value to be more than one of the types in the union.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| types | A list of types the value can be. | *List of Recap type objects* | YES | |

#### Examples

```yaml
# A union type of null or a 32-bit signed int
type: union
types:
  - type: null
  - type: int
    bits: 32
```

Unions can also be defined as a list of types:

```yaml
# A union type of null or a boolean
type: ["null", "bool"]
```

## Documentation

All types support a `doc` attribute, which allows developers to document types.

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| doc | A documentation string for the type. | A string \| null | NO | null |

```yaml
type: union
doc: A union type of null or a 32-bit signed int
types:
    - type: null
    - type: int
      bits: 32
```

## Aliases

All types support an `alias` attribute. `alias` allows a type to reference previously defined types. This is handy in larger data structures where complex types are repeatedl used.

Aliases are globally unique, so they must include a unique dotted namespace prefix. Naked aliases (aliases with no dotted namespace) are reserved for Recap's built-in logical types (see the next section).

### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| alias | An alias for the type. | A string \| null | NO | null |

### Examples

Aliases are referenced using the `type` field:

```yaml
type: struct
doc: A book with pages
fields:
  - name: previous
    alias: com.mycorp.models.Page
    type: int
    bits: 32
    signed: false
  - name: next
    type: com.mycorp.models.Page
```

Recap will treat this struct the same as:

```yaml
type: struct
doc: A book with pages
fields:
  - name: previous
    alias: com.mycorp.models.Page
    type: int
    bits: 32
    signed: false
  # The `next` field has the same types and attributes
  # as the `previous` one; they're both pages.
  - name: next
    type: int
    bits: 32
    signed: false
```

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

And attribute overrides:

```yaml
type: struct
fields:
  - name: id
    alias: com.mycorp.models.Uint24
    type: int
    bits: 24
    signed: false
  - name: signed_id
    type: com.mycorp.models.Uint24
    # Let's make this a signed int24
    signed: true
```

But alias inheritance is not allowed:

```yaml
type: struct
doc: All fields have the same type
fields:
  - name: field1
    alias: "com.mycorp.models.Field"
    type: int
    bits: 32
    signed: false
  - name: field2
    type: com.mycorp.models.Field
    # An alias of an alias isn't allowed.
    alias: com.mycorp.models.FieldAlias
  - name: field3
    type: com.mycorp.models.FieldAlias
```

## Logical Types

Logical types extend one of the 11 Recap types listed in the *Types* section at the top of the spec. Logical types make Recap's type system extensible. For example, a `uint8` type can be defined as: 

```yaml
type: int
alias: uint8
bits: 8
signed: false
```

Similar to aliases, logical types can be referenced using the `type` attribute.

### Built-in Logical Types

Recap comes with a collection of built-in logical types:

* `int8`: An 8-bit signed integer.
* `uint8`: An 8-bit unsigned integer.
* `int16`: A 16-bit signed integer.
* `uint16`: A 16-bit unsigned integer.
* `int32`: A 32-bit signed integer.
* `uint32`: A 32-bit unsigned integer.
* `int64`: A 64-bit signed integer.
* `uint64`: A 64-bit unsigned integer.
* `float16`: A 16-bit IEEE 754 encoded floating point number.
* `float32`: A 32-bit IEEE 754 encoded floating point number.
* `float64`: A 64-bit IEEE 754 encoded floating point number.
* `string32`: A variable-length UTF-8 encoded unicode string with a maximum length of 2_147_483_648.
* `string64`: A variable-length UTF-8 encoded unicode string with a maximum length of 9_223_372_036_854_775_807.
* `bytes32`: A variable-length byte array with a maximum length of 2_147_483_648.
* `bytes64`: A variable-length byte array with a maximum length of 9_223_372_036_854_775_807.
* `uuid`: A fixed-length 36 byte string in UUID 8-4-4-4-12 string format as defined in [RFC 4122](https://www.ietf.org/rfc/rfc4122.txt).
* `decimal`: An arbitrary-precision decimal number.
* `decimal128`: An arbitrary-precision decimal number stored in a fixed-length 128-bit byte array.
* `decimal256`: An arbitrary-precision decimal number stored in a fixed-length 256-bit byte array.
* `duration64`: A length of time and time unit without timezones and leap seconds.
* `interval128`: An interval of time on a calendar measured in months, days, and a duration of intra-day time with a time unit.
* `time32`: Time since midnight without timezones and leap seconds in a 32-bit signed integer.
* `time64`: Time since midnight without timezones and leap seconds in a 64-bit signed integer.
* `timestamp64`: Time elapsed (in a time unit) since a specific epoch.
* `date32`: Date since the UNIX epoch without timezones and leap seconds in a 32-bit integer.
* `date64`: Date since the UNIX epoch without timezones and leap seconds in a 64-bit integer.

Most of these are self explanatory, but the decimal and time types are explained in more detail below.

### `decimal`

An arbitrary-precision decimal number. This type is the same as [Avro's Decimal](https://avro.apache.org/docs/1.10.2/spec.html#Decimal).

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| precision | Total number of digits. 123.456 has a precision of 6. | 32-bit signed integer | YES | |
| scale | Digits to the right of the decimal point. 123.456 has a scale of 3. | 32-bit signed integer | YES | |

#### Definition

```yaml
type: bytes32
alias: decimal
```

### `decimal128`

An arbitrary-precision decimal number stored in a fixed-length 128-bit byte array. This type is the same as [Arrow's decimal128](https://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html#pyarrow.decimal128).

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| precision | Total number of digits. 123.456 has a precision of 6. | 32-bit signed integer | YES | |
| scale | Digits to the right of the decimal point. 123.456 has a scale of 3. | 32-bit signed integer | YES | |

#### Definition

```yaml
type: bytes
alias: decimal128
bytes: 16
variable: false
```

### `decimal256`

An arbitrary-precision decimal number stored in a fixed-length 256-bit byte array. This type is the same as [Arrow's decimal256](https://arrow.apache.org/docs/r/reference/data-type.html).

##### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| precision | Total number of digits. 123.456 has a precision of 6. | 32-bit signed integer | YES | |
| scale | Digits to the right of the decimal point. 123.456 has a scale of 3. | 32-bit signed integer | YES | |

#### Definition

```yaml
type: bytes
alias: decimal256
bytes: 32
variable: false
```

### `duration64`

A length of time without timezones and leap seconds. This type is the same as [Arrow's Duration](https://arrow.apache.org/docs/python/generated/pyarrow.duration.html) but with a superset of time units.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| unit | A string time unit. | String literal of YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND | YES | |

#### Definition

```yaml
type: int64
alias: duration64
```

### `interval128`

An interval of time on a calendar. This measurement allows you to measure time without worrying about leap seconds, leap years, and time changes. Years, quarters, hours, and minutes can be expressed using this type.

The interval is measured in months, days, and an intra-day time measurement. Months and days are each 32-bit signed integers. The remainder is a 64-bit signed integer measured in a certain time unit. Leap seconds are ignored.

This type is the same as [Avro's Duration](https://avro.apache.org/docs/1.10.2/spec.html#Duration) but with a superset of time units.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| unit | A string time unit. | String literal of YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND | YES | |

#### Definition

```yaml
type: bytes
alias: interval128
bytes: 16
variable: false
```

### `time32`

Time since midnight without timezones and leap seconds in a 32-bit integer. This type is the same as [Arrow's time32](https://arrow.apache.org/docs/python/generated/pyarrow.time32.html) but with a superset of time units.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| unit | A string time unit. | String literal of YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND | YES | |

#### Definition

```yaml
type: int32
alias: time32
```

#### `time64`

Time since midnight without timezones and leap seconds in a 64-bit integer. This type is the same as [Arrow's time64](https://arrow.apache.org/docs/python/generated/pyarrow.time64.html) but with a superset of time units.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| unit | A string time unit. | String literal of YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND | YES | |

#### Definition

```yaml
type: int64
alias: time64
```

### `timestamp64`

Time elapsed since a specific epoch.

A timestamp with no timezone is a `datetime` in database parlance--a date and time as you would see it on wrist-watch.

A timestamp with a timezone represents the amount of time elapsed since the 1970-01-01 00:00:00 epoch in UTC time zone (regardless of the timezone that's specified). Readers must translate the UTC timestamp to a timestamp value for the specified timezone. See Apache Arrow's [Schema.fbs](https://github.com/apache/arrow/blob/main/format/Schema.fbs) documentation for more details.

This type is the same as [Arrow's timestamp](https://arrow.apache.org/docs/python/generated/pyarrow.timestamp.html) but with a superset of time units.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| unit | A string time unit. | String literal of YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND | YES | |
| timezone | An optional [Olson timezone database](https://en.wikipedia.org/wiki/Tz_database) string. | A string \| null | NO | null |

#### Definition

```yaml
type: int64
alias: timetamp64
```

### `date32`

Date since the UNIX epoch without timezones and leap seconds in a 32-bit integer. This type is the same as [Arrow's date32](https://arrow.apache.org/docs/python/generated/pyarrow.date32.html) but with a superset of time units.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| unit | A string time unit. | String literal of YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND | YES | |

#### Definition

```yaml
type: int32
alias: date32
```

### `date64`

Date since the UNIX epoch without timezones and leap seconds in a 64-bit integer. This type is the same as [Arrow's date64](https://arrow.apache.org/docs/python/generated/pyarrow.date64.html) but with a superset of time units.

#### Attributes

| Name | Description | Type | Required | Default |
|------|-------------|------|----------|---------|
| unit | A string time unit. | String literal of YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, or PICOSECOND | YES | |

#### Definition

```yaml
type: int64
alias: date64
```

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

### Why didn't you use Apache Arrow?

For starters, Arrow doesn't have a schema definition language; it's entirely programmatic. Their spec is their [Schema.fbs](https://github.com/apache/arrow/blob/main/format/Schema.fbs). You'd have to write code to define a schema; this isn't what I wanted.

In addition, their in-memory model is more optimized for columnar analytical work. Recap's main focus is on record-based data from APIs, message buses, and databases. Single record operations and schema manipulation tend to be different than analytical operations. Counting and grouping doesn't make much sense on a single record. Schema manipulation is where Recap shines: projecting out fields, merging and intersecting schemas, and checking for schema compatibility.

### How do you handle coercion?

Coercing types that don't fit cleanly into Recap's type system is outside the scope of this spec. Recap will include a test suite that defines how specific types are meant to be converted to and from Recap types on a per-SDL basis. For example, converting PostgreSQL's `geography(point)` type to/from a Recap type is something that will be defined in the test suite and documented in the PostgreSQL converter documentation.
