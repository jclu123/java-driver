## Custom codecs

### Quick overview

Define custom Java to CQL mappings.

* implement the [TypeCodec] interface.
* registering a codec:
  * at init time: [CqlSession.builder().addTypeCodecs()][SessionBuilder.addTypeCodecs]
  * at runtime:
  
    ```java
    MutableCodecRegistry registry =
        (MutableCodecRegistry) session.getContext().getCodecRegistry();    
    registry.register(myCodec);
    ```
* using a codec:
  * if already registered: `row.get("columnName", MyCustomType.class)`
  * otherwise: `row.get("columnName", myCodec)`

-----

Out of the box, the driver comes with [default CQL to Java mappings](../#cql-to-java-type-mapping).
For example, if you read a CQL `text` column, it is mapped to its natural counterpart
`java.lang.String`:

```java
// cqlsh:ks> desc table test;
// CREATE TABLE ks.test (k int PRIMARY KEY, v text)...
ResultSet rs = session.execute("SELECT * FROM ks.test WHERE k = 1");
String v = rs.one().getString("v");
```

Sometimes you might want to use different mappings, for example:

* read a text column as a Java enum;
* map an `address` UDT to a custom `Address` class in your application;
* manipulate CQL collections as arrays in performance-intensive applications.

Custom codecs allow you to define those dedicated mappings, and plug them into your session.

### Using alternative codecs provided by the driver

The first thing you can do is use one of the many alternative codecs shipped with the driver. In 
this section we are going to introduce these codecs, then you will see how to register and use them 
in the next sections.

#### Codecs for CQL blobs

You can choose between 2 different codecs for handling the CQL `blob` type:

1. The driver default is [TypeCodecs.BLOB], which maps CQL blobs to Java's [java.nio.ByteBuffer]. 
   Check out our [CQL `blob` example] to understand how to manipulate the `ByteBuffer` API 
   correctly.
2. If the `ByteBuffer` API is too cumbersome for you, an alternative is to use 
   [TypeCodecs.BLOB_SIMPLE] which maps CQL blobs to Java primitive byte arrays (`byte[]`).

Note: There is also [TypeCodecs.BYTE_ARRAY] which converts `list<tinyint>` to `byte[]` (see below).

#### Mapping CQL lists to Java arrays

By default, the driver maps CQL `list` to Java's [java.util.List]. If you prefer to deal with 
arrays, the driver offers the following codecs:

1. For primitive types:
    1. [TypeCodecs.BOOLEAN_ARRAY] maps `list<boolean>` to `boolean[]`;
    1. [TypeCodecs.BYTE_ARRAY] maps `list<tinyint>` to `byte[]`;
    2. [TypeCodecs.SHORT_ARRAY] maps `list<smallint>` to `short[]`;
    3. [TypeCodecs.INT_ARRAY] maps `list<int>` to `int[]`;
    4. [TypeCodecs.LONG_ARRAY] maps `list<bigint>` to `long[]`;
    5. [TypeCodecs.FLOAT_ARRAY] maps `list<float>` to `float[]`;
    6. [TypeCodecs.DOUBLE_ARRAY] maps `list<double>` to `double[]`;
2. For other types, you should use [TypeCodecs.arrayOf(TypeCodec)]; for example, to map CQL 
   `list<text>` to `String[]`:

    ```java
    TypeCodec<String[]> stringArrayCodec = TypeCodecs.arrayOf(TypeCodecs.TEXT);
    ```

#### Mapping CQL timestamps and handling time zones

By default, the driver maps CQL `timestamp` to Java's [java.time.Instant]. `Instant` is the most 
natural type for CQL `timestamp` since timestamps do not contain any time zone information, just as 
`Instant`: they are just points in time.

However most applications have to deal with time zones. The driver offers the following alternatives 
to handle time zones in many ways:

1. Zone-agnostic codecs
    1. As we said, the default codec is [TypeCodecs.TIMESTAMP] which maps `timestamp` to `Instant`.
    This codec is mostly agnostic of time zones, except in one particular case: when parsing CQL 
    literals, if the literal does not contain any time zone, this codec will assume the system's 
    default time zone.
    2. An alternative is [TypeCodecs.TIMESTAMP_UTC]. It also maps `timestamp` to `Instant`, the only 
    difference is that when parsing CQL literals, if the literal does not contain any time zone, this 
    codec will rather assume UTC.
    3. If when parsing CQL literals you want the codec to assume a different default zone, use
    [TypeCodecs.timestampAt(ZoneId)], e.g. 
    
        ```java
        TypeCodec<Instant> codec = TypeCodecs.timestampAt(ZoneOffset.ofHours(2))
        ```
    4. If your application prefers to deal with raw milliseconds since the Epoch, then you can use
    [TypeCodecs.TIMESTAMP_MILLIS_SYSTEM] and [TypeCodecs.TIMESTAMP_MILLIS_UTC]: both convert CQL
    `timestamp` to primitive `long`. There is also [TypeCodecs.timestampMillisAt(ZoneId)] if you
    need a different default zone when parsing.
2. Zone-aware codecs
    1. If your application works with one single, pre-determined time zone, then you probably would 
    like the driver to map `timestamp` to [java.time.ZonedDateTime] with a fixed zone. Use one of 
    the following codecs:
        1. [TypeCodecs.ZONED_TIMESTAMP_SYSTEM] will convert all CQL timestamps to 
        [java.time.ZonedDateTime] using the system's default time zone.
        2. [TypeCodecs.ZONED_TIMESTAMP_UTC] will all convert CQL timestamps to 
        [java.time.ZonedDateTime] using UTC.
        3. [TypeCodecs.zonedTimestampAt(ZoneId)] can be used to create codecs for  
        [java.time.ZonedDateTime] using other time zones.
    2. If your application works with one single, pre-determined time zone, but only exposes local
    date-times, then you probably would like the driver to map timestamps to 
    [java.time.LocalDateTime] obtained from a fixed zone. Use one of the following codecs:
        1. [TypeCodecs.LOCAL_TIMESTAMP_SYSTEM] will convert all CQL timestamps to 
        [java.time.LocalDateTime] using the system's default time zone.
        2. [TypeCodecs.LOCAL_TIMESTAMP_UTC] will all convert CQL timestamps to 
        [java.time.LocalDateTime] using UTC.
        3. [TypeCodecs.localTimestampAt(ZoneId)] can be used to create codecs for  
        [java.time.LocalDateTime] using other time zones.
    3. If your application works with multiple time zones, then you probably would like the driver 
    to map `timestamp` to [java.time.ZonedDateTime], but you will need a way to persist the time 
    zone to the database. We suggest that zoned timestamps be stored in the database as 
    `tuple<timestamp,text>`, where the first component in the tuple stores the `Instant` part as a 
    CQL `timestamp`, and the second component in the tuple stores the [ZoneId] part as a CQL `text`. 
    If you follow this guideline when creating your table schemas, then you can use 
    [TypeCodecs.ZONED_TIMESTAMP_PERSISTED] to persist zoned timestamps in Apache Cassandra out of 
    the box.
 
#### Mapping to `Optional` instead of `null` 

If you prefer to deal with [java.util.Optional] in your application instead of nulls, then you can 
use [TypeCodecs.optionalOf(TypeCodec)]:

```java
TypeCodec<Optional<UUID>> optionalUuidCodec = TypeCodecs.optionalOf(TypeCodecs.UUID);
```

Note that because the CQL native protocol does not distinguish empty collections from null 
collection references, this codec will also map empty collections to [Optional.empty()].

#### Mapping Java Enums

Java [Enums] can be mapped to CQL in two ways:

1. By name: [TypeCodecs.enumNamesOf(Class)] will create a codec for a given `Enum` class that maps 
its constants to their [programmatic names][Enum.name()]. The corresponding CQL column must be of 
type `text`. Note that this codec relies on the enum constant names; it is therefore vital that enum 
names never change.
1. By ordinal: [TypeCodecs.enumOrdinalsOf(Class)] will create a codec for a given `Enum` class that 
maps its constants to their [ordinal value][Enum.ordinal()]. The corresponding CQL column must be of 
type `int`. Note that this codec relies on the enum constants declaration order; it is therefore 
vital that this order never change.

For example, assuming the following enum:

```java
public enum WeekDay {
  MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY 
}
```

You can define codecs for it the following ways:

```java
// MONDAY will be persisted as "MONDAY", TUESDAY as "TUESDAY", etc.
TypeCodec<String> weekDaysByNameCodec = TypeCodecs.enumNamesOf(WeekDay.class);

// MONDAY will be persisted as 0, TUESDAY as 1, etc.
TypeCodec<Integer> weekDaysByNameCodec = TypeCodecs.enumOrdinalsOf(WeekDay.class);
```

#### Mapping Json

The driver provides out-of-the-box support for mapping Java objects to CQL `text` using the popular
Jackson library. The method [TypeCodecs.json(Class)] will create a codec for a given Java class that
maps instances of that class to Json strings, using a newly-allocated, default [ObjectMapper]. It is 
also possible to pass a custom `ObjectMapper` instance using [TypeCodecs.json(Class, ObjectMapper)]
instead.

### Writing codecs

If none of the driver built-in codecs above suits you, it is also possible to roll your own.

To write a custom codec, implement the [TypeCodec] interface. Here is an example that maps a CQL
`int` to a Java string containing its textual representation:

```java
public class CqlIntToStringCodec implements TypeCodec<String> {

  @Override
  public GenericType<String> getJavaType() {
    return GenericType.STRING;
  }

  @Override
  public DataType getCqlType() {
    return DataTypes.INT;
  }

  @Override
  public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    } else {
      int intValue = Integer.parseInt(value);
      return TypeCodecs.INT.encode(intValue, protocolVersion);
    }
  }

  @Override
  public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    Integer intValue = TypeCodecs.INT.decode(bytes, protocolVersion);
    return intValue.toString();
  }

  @Override
  public String format(String value) {
    int intValue = Integer.parseInt(value);
    return TypeCodecs.INT.format(intValue);
  }

  @Override
  public String parse(String value) {
    Integer intValue = TypeCodecs.INT.parse(value);
    return intValue == null ? null : intValue.toString();
  }
}
```

Admittedly, this is a trivial -- and maybe not very realistic -- example, but it illustrates a few
important points:
 
* which methods to override. Refer to the [TypeCodec] javadocs for additional information about each
  of them; 
* how to piggyback on a built-in codec, in this case `TypeCodecs.INT`. Very often, this is the best
  approach to keep the code simple. If you want to handle the binary encoding yourself (maybe to
  squeeze the last bit of performance), study the driver's
  [built-in codec implementations](https://github.com/datastax/java-driver/tree/4.x/core/src/main/java/com/datastax/oss/driver/internal/core/type/codec). 

### Using codecs

Once you have your codec, register it when building your session. The following example registers
`CqlIntToStringCodec` along with a few driver-supplied alternative codecs:

```java
CqlSession session =
  CqlSession.builder()
    .addTypeCodecs(
      new CqlIntToStringCodec(),              // user-created codec
      TypeCodecs.ZONED_TIMESTAMP_PERSISTED,   // tuple<timestamp,text> <-> ZonedDateTime
      TypeCodecs.BLOB_SIMPLE,                 // blob <-> byte[]
      TypeCodecs.arrayOf(TypeCodecs.TEXT),    // list<text> <-> String[]
      TypeCodecs.enumNamesOf(WeekDay.class),  // text <-> WeekDay
      TypeCodecs.json(MyJsonPojo.class)       // text <-> MyJsonPojo
      TypeCodecs.optionalOf(TypeCodecs.UUID)  // uuid <-> Optional<UUID>
    )
    .build();
```

You may also add codecs to an existing session at runtime:

```java
// The cast is required for backward compatibility reasons (registry mutability was introduced in
// 4.3.0). It is safe as long as you didn't hack the driver internals to plug a custom registry
// implementation.
MutableCodecRegistry registry =
    (MutableCodecRegistry) session.getContext().getCodecRegistry();

registry.register(new CqlIntToStringCodec());
```

You can now use the new `int <-> String` mapping in your code:

```java
// cqlsh:ks> desc table test2;
// CREATE TABLE ks.test2 (k int PRIMARY KEY, v int)...
ResultSet rs = session.execute("SELECT * FROM ks.test2 WHERE k = 1");
String v = rs.one().getString("v"); // read a CQL int as a java.lang.String

PreparedStatement ps = session.prepare("INSERT INTO ks.test2 (k, v) VALUES (?, ?)");
session.execute(
    ps.boundStatementBuilder()
        .setInt("k", 2)
        .setString("v", "12") // write a java.lang.String as a CQL int
        .build());
```

In the above example, the driver will look up in the codec registry a codec for CQL `int` and Java
String, and will transparently pick `CqlIntToStringCodec` for that.

So far our examples have used a Java type with dedicated accessors in the driver: `getString` and
`setString`. But sometimes you won't find suitable accessor methods; for example, there is no 
accessor for `ZonedDateTime` or for `Optional<UUID>`, and yet we registered codecs for these types. 

When you want to retrieve such objects, you need a way to tell the driver which Java type you want.
You do so by using one of the generic `get` and `set` methods:

```java
// Assuming that TypeCodecs.ZONED_TIMESTAMP_PERSISTED was registered
// Assuming that TypeCodecs.BLOB_SIMPLE was registered
// Assuming that TypeCodecs.arrayOf(TypeCodecs.TEXT) was registered

// Reading
ZonedDateTime v1 = row.get("v1", ZonedDateTime.class); // assuming column is of type timestamp
byte[] v2        = row.get("v2", byte[].class);        // assuming column is of type blob
String[] v3      = row.get("v3", String[].class);      // assuming column is of type list<text>


// Writing
boundStatement.set("v1", v1, ZonedDateTime.class);
boundStatement.set("v2", v2, byte[].class);
boundStatement.set("v3", v3, String[].class);
```

This is also valid for arbitrary Java types. This is particularly useful when dealing with Enums and 
Json mappings. For example, let's assume you have a `Price` class, and have registered a Json codec 
that maps it to CQL `text`:

```java
// Assuming that TypeCodecs.enumNamesOf(WeekDay.class) was registered
// Assuming that TypeCodecs.json(Price.class) was registered

// Reading
WeekDay v1 = row.get("v1", WeekDay.class); // assuming column is of type text
Price v2   = row.get("v2", Price.class);   // assuming column is of type text

// Writing
boundStatement.set("v1", v1, WeekDay.class);
boundStatement.set("v2", v2, Price.class);
```

Note that, because the underlying CQL type is `text` you can still retrieve the column's contents
as a plain string:

```java
// Reading
String enumName = row.getString("v1");
String priceJson = row.getString("v2");

// Writing
boundStatement.setString("v1", enumName);
boundStatement.setString("v2", priceJson);
```

And finally, for `Optional<UUID>`, you will need the `get` and `set` methods with an extra *type 
token* argument, because `Optional<UUID>` is a parameterized type:

```java
// Assuming that TypeCodecs.optionalOf(TypeCodecs.UUID) was registered

// Reading
Optional<UUID> opt = row.get("v", GenericType.optionalOf(UUID.class));

// Writing
boundStatement.set("v", opt, GenericType.optionalOf(UUID.class));
```

Type tokens are instances of [GenericType]. They are immutable and thread-safe, you should store
them as reusable constants. The `GenericType` class itself has constants and factory methods to help
creating `GenericType` objects for common types. If you don't see the type you are looking for, a
type token for any Java type can be created using the following pattern:

```java
// Notice the '{}': this is an anonymous inner class
GenericType<Foo<Bar>> fooBarType = new GenericType<Foo<Bar>>(){};

Foo<Bar> v = row.get("v", fooBarType);
```

Custom codecs are used not only for their base type, but also recursively in collections, tuples and
UDTs. For example, once your Json codec for the `Price` class is registered, you can also read a CQL
`list<text>` as a Java `List<Price>`:

```java
// Assuming that TypeCodecs.json(Price.class) was registered
// Assuming that each element of the list<text> column is a valid Json string

// Reading
List<Price> prices1 = row.getList("v", Price.class);
// alternative method using the generic get method with type token argument:
List<Price> prices2 = row.get("v", GenericType.listOf(Price.class));

// Writing
boundStatement.setList("v", prices1, Price.class);
// alternative method using the generic set method with type token argument:
boundStatement.set("v", prices2, GenericType.listOf(Price.class));
``` 

Whenever you read or write a value, the driver tries all the built-in mappings first, followed by
custom codecs. If two codecs can process the same mapping, the one that was registered first is
used. Note that this means that built-in mappings can't be overridden.

In rare cases, you might have a codec registered in your application, but have a legitimate reason
to use a different mapping in one particular place. In that case, you can pass a codec instance 
to `get` / `set` instead of a type token:

```java
TypeCodec<String> defaultCodec = new CqlIntToStringCodec();
TypeCodec<String> specialCodec = ...; // a different implementation

CqlSession session =
    CqlSession.builder().addTypeCodecs(defaultCodec).build();

String s1 = row.getString("anIntColumn");         // int -> String, will decode with defaultCodec
String s2 = row.get("anIntColumn", specialCodec); // int -> String, will decode with specialCodec
``` 

By doing so, you bypass the codec registry completely and instruct the driver to use the given 
codec. Note that it is your responsibility to ensure that the codec can handle the underlying CQL
type (this cannot be enforced at compile-time).

### Creating custom Java-to-CQL mappings with `MappingCodec`

The above example, `CqlIntToStringCodec`, could be rewritten to leverage [MappingCodec], an abstract 
class that ships with the driver. This class has been designed for situations where we want to 
represent a CQL type with a different Java type than the Java type natively supported by the driver,
and the conversion between the former and the latter is straightforward. 

All you have to do is extend `MappingCodec` and implement two methods that perform the conversion 
between the supported Java type -- or "inner" type -- and the target Java type -- or "outer" type:

```java
public class CqlIntToStringCodec extends MappingCodec<Integer, String> {

  public CqlIntToStringCodec() {
    super(TypeCodecs.INT, GenericType.STRING);
  }

  @Nullable
  @Override
  protected String innerToOuter(@Nullable Integer value) {
    return value == null ? null : value.toString();
  }

  @Nullable
  @Override
  protected Integer outerToInner(@Nullable String value) {
    return value == null ? null : Integer.parseInt(value);
  }
}
```

This technique is especially useful when mapping user-defined types to Java objects. For example, 
let's assume the following user-defined type:

```
CREATE TYPE coordinates (x int, y int);
 ```
 
And let's suppose that we want to map it to the following Java class:
 
```java
public class Coordinates {
  public final int x;
  public final int y;
  public Coordinates(int x, int y) { this.x = x; this.y = y; }
}
```

All  you have to do is create a `MappingCodec` subclass that piggybacks on an existing 
`TypeCodec<UdtValue>` for the above user-defined type:

```java
public class CoordinatesCodec extends MappingCodec<UdtValue, Coordinates> {

  public CoordinatesCodec(@NonNull TypeCodec<UdtValue> innerCodec) {
    super(innerCodec, GenericType.of(Coordinates.class));
  }

  @NonNull @Override public UserDefinedType getCqlType() {
    return (UserDefinedType) super.getCqlType();
  }

  @Nullable @Override protected Coordinates innerToOuter(@Nullable UdtValue value) {
    return value == null ? null : new Coordinates(value.getInt("x"), value.getInt("y"));
  }

  @Nullable @Override protected UdtValue outerToInner(@Nullable Coordinates value) {
    return value == null ? null : getCqlType().newValue().setInt("x", value.x).setInt("y", value.y);
  }
}
```

Then the new mapping codec could be registered as follows:

```java
CqlSession session = ...
CodecRegistry codecRegistry = session.getContext().getCodecRegistry();
// The target user-defined type
UserDefinedType coordinatesUdt =
    session
        .getMetadata()
        .getKeyspace("...")
        .flatMap(ks -> ks.getUserDefinedType("coordinates"))
        .orElseThrow(IllegalStateException::new);
// The "inner" codec that handles the conversions from CQL from/to UdtValue
TypeCodec<UdtValue> innerCodec = codecRegistry.codecFor(coordinatesUdt);
// The mapping codec that will handle the conversions from/to UdtValue and Coordinates
CoordinatesCodec coordinatesCodec = new CoordinatesCodec(innerCodec);
// Register the new codec
((MutableCodecRegistry) codecRegistry).register(coordinatesCodec);
```

...and used just like explained above:

```java
BoundStatement stmt = ...;
stmt.set("coordinates", new Coordinates(10,20), Coordinates.class);

Row row = ...;
Coordinates coordinates = row.get("coordinates", Coordinates.class);
``` 

Note: if you need even more advanced mapping capabilities, consider adopting
the driver's [object mapping framework](../../mapper/).

### Subtype polymorphism

Suppose the following class hierarchy:

```java
class Animal {}
class Cat extends Animal {}
```

By default, a codec will accept to serialize any object that extends or implements its declared Java
type: a codec such as `AnimalCodec extends TypeCodec<Animal>` will accept `Cat` instances as well.

This allows a codec to handle interfaces and superclasses in a generic way, regardless of the actual
implementation being used by client code; for example, the driver has a built-in codec that handles
`List` instances, and this codec is capable of serializing any concrete `List` implementation.

But this has one caveat: when setting or retrieving values with `get()` and `set()`, *you must pass
the exact Java type the codec handles*:

```java
BoundStatement bs = ...
bs.set(0, new Cat(), Animal.class); // works
bs.set(0, new Cat(),    Cat.class); // throws CodecNotFoundException

Row row = ...
Animal animal = row.get(0, Animal.class); // works
Cat    cat    = row.get(0,    Cat.class); // throws CodecNotFoundException
```

### The codec registry

The driver stores all codecs (built-in and custom) in an internal [CodecRegistry]:

```java
CodecRegistry getCodecRegistry = session.getContext().getCodecRegistry();

// Get the custom codec we registered earlier:
TypeCodec<String> cqlIntToString = codecRegistry.codecFor(DataTypes.INT, GenericType.STRING);
```

If all you're doing is executing requests and reading responses, you probably won't ever need to
access the registry directly. But it's useful if you do some kind of generic processing, for
example printing out an arbitrary row when the schema is not known at compile time:

```java
private static String formatRow(Row row) {
  StringBuilder result = new StringBuilder();
  for (int i = 0; i < row.size(); i++) {
    String name = row.getColumnDefinitions().get(i).getName().asCql(true);
    Object value = row.getObject(i);
    DataType cqlType = row.getType(i);
    
    // Find the best codec to format this CQL type: 
    TypeCodec<Object> codec = row.codecRegistry().codecFor(cqlType);

    if (i != 0) {
      result.append(", ");
    }
    result.append(name).append(" = ").append(codec.format(value));
  }
  return result.toString();
}
```

[CodecRegistry]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/registry/CodecRegistry.html
[GenericType]:   https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/reflect/GenericType.html
[TypeCodec]:     https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodec.html
[MappingCodec]:     https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/MappingCodec.html
[SessionBuilder.addTypeCodecs]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addTypeCodecs-com.datastax.oss.driver.api.core.type.codec.TypeCodec...-

[Enums]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html
[Enum.name()]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html#name--
[Enum.ordinal()]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html#ordinal--
[java.nio.ByteBuffer]: https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
[java.util.List]: https://docs.oracle.com/javase/8/docs/api/java/util/List.html
[java.util.Optional]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
[Optional.empty()]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html#empty--
[java.time.Instant]: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html
[java.time.ZonedDateTime]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html
[java.time.LocalDateTime]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html
[java.time.ZoneId]: https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html

[TypeCodecs.BLOB]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#BLOB
[TypeCodecs.BLOB_SIMPLE]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#BLOB_SIMPLE
[TypeCodecs.BOOLEAN_ARRAY]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#BOOLEAN_ARRAY
[TypeCodecs.BYTE_ARRAY]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#BYTE_ARRAY
[TypeCodecs.SHORT_ARRAY]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#SHORT_ARRAY
[TypeCodecs.INT_ARRAY]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#INT_ARRAY
[TypeCodecs.LONG_ARRAY]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#LONG_ARRAY
[TypeCodecs.FLOAT_ARRAY]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#FLOAT_ARRAY
[TypeCodecs.DOUBLE_ARRAY]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#DOUBLE_ARRAY
[TypeCodecs.arrayOf(TypeCodec)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#arrayOf-com.datastax.oss.driver.api.core.type.codec.TypeCodec-
[TypeCodecs.TIMESTAMP]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#TIMESTAMP
[TypeCodecs.TIMESTAMP_UTC]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#TIMESTAMP_UTC
[TypeCodecs.timestampAt(ZoneId)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#timestampAt-java.time.ZoneId-
[TypeCodecs.TIMESTAMP_MILLIS_SYSTEM]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#TIMESTAMP_MILLIS_SYSTEM
[TypeCodecs.TIMESTAMP_MILLIS_UTC]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#TIMESTAMP_MILLIS_UTC
[TypeCodecs.timestampMillisAt(ZoneId)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#timestampMillisAt-java.time.ZoneId-
[TypeCodecs.ZONED_TIMESTAMP_SYSTEM]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#ZONED_TIMESTAMP_SYSTEM
[TypeCodecs.ZONED_TIMESTAMP_UTC]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#ZONED_TIMESTAMP_UTC
[TypeCodecs.zonedTimestampAt(ZoneId)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#zonedTimestampAt-java.time.ZoneId-
[TypeCodecs.LOCAL_TIMESTAMP_SYSTEM]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#LOCAL_TIMESTAMP_SYSTEM
[TypeCodecs.LOCAL_TIMESTAMP_UTC]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#LOCAL_TIMESTAMP_UTC
[TypeCodecs.localTimestampAt(ZoneId)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#localTimestampAt-java.time.ZoneId-
[TypeCodecs.ZONED_TIMESTAMP_PERSISTED]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#ZONED_TIMESTAMP_PERSISTED
[TypeCodecs.optionalOf(TypeCodec)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#optionalOf-com.datastax.oss.driver.api.core.type.codec.TypeCodec-
[TypeCodecs.enumNamesOf(Class)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#enumNamesOf-java.lang.Class-
[TypeCodecs.enumOrdinalsOf(Class)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#enumOrdinalsOf-java.lang.Class-
[TypeCodecs.json(Class)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#json-java.lang.Class-
[TypeCodecs.json(Class, ObjectMapper)]: https://docs.datastax.com/en/drivers/java/4.8/com/datastax/oss/driver/api/core/type/codec/TypeCodecs.html#json-java.lang.Class-com.fasterxml.jackson.databind.ObjectMapper-

[ObjectMapper]: http://fasterxml.github.io/jackson-databind/javadoc/2.10/com/fasterxml/jackson/databind/ObjectMapper.html

[CQL `blob` example]: https://github.com/datastax/java-driver/blob/4.x/examples/src/main/java/com/datastax/oss/driver/examples/datatypes/Blobs.java