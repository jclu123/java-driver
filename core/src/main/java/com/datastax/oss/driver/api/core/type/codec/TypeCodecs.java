/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.type.codec;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.BigIntCodec;
import com.datastax.oss.driver.internal.core.type.codec.BlobCodec;
import com.datastax.oss.driver.internal.core.type.codec.BooleanCodec;
import com.datastax.oss.driver.internal.core.type.codec.CounterCodec;
import com.datastax.oss.driver.internal.core.type.codec.CqlDurationCodec;
import com.datastax.oss.driver.internal.core.type.codec.CustomCodec;
import com.datastax.oss.driver.internal.core.type.codec.DateCodec;
import com.datastax.oss.driver.internal.core.type.codec.DecimalCodec;
import com.datastax.oss.driver.internal.core.type.codec.DoubleCodec;
import com.datastax.oss.driver.internal.core.type.codec.FloatCodec;
import com.datastax.oss.driver.internal.core.type.codec.InetCodec;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import com.datastax.oss.driver.internal.core.type.codec.ListCodec;
import com.datastax.oss.driver.internal.core.type.codec.MapCodec;
import com.datastax.oss.driver.internal.core.type.codec.SetCodec;
import com.datastax.oss.driver.internal.core.type.codec.SimpleBlobCodec;
import com.datastax.oss.driver.internal.core.type.codec.SmallIntCodec;
import com.datastax.oss.driver.internal.core.type.codec.StringCodec;
import com.datastax.oss.driver.internal.core.type.codec.TimeCodec;
import com.datastax.oss.driver.internal.core.type.codec.TimeUuidCodec;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import com.datastax.oss.driver.internal.core.type.codec.TinyIntCodec;
import com.datastax.oss.driver.internal.core.type.codec.TupleCodec;
import com.datastax.oss.driver.internal.core.type.codec.UdtCodec;
import com.datastax.oss.driver.internal.core.type.codec.UuidCodec;
import com.datastax.oss.driver.internal.core.type.codec.VarIntCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.OptionalCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.BooleanArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.ByteArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.DoubleArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.FloatArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.IntArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.LongArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.ObjectArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.ShortArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.enums.EnumNameCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.enums.EnumOrdinalCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.json.JsonCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.LocalTimestampCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.PersistentZonedTimestampCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.TimestampMillisCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.ZonedTimestampCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.fasterxml.jackson.databind.json.JsonMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Constants and factory methods to obtain type codec instances. */
public class TypeCodecs {

  public static final PrimitiveBooleanCodec BOOLEAN = new BooleanCodec();
  public static final PrimitiveByteCodec TINYINT = new TinyIntCodec();
  public static final PrimitiveDoubleCodec DOUBLE = new DoubleCodec();
  public static final PrimitiveLongCodec COUNTER = new CounterCodec();
  public static final PrimitiveFloatCodec FLOAT = new FloatCodec();
  public static final PrimitiveIntCodec INT = new IntCodec();
  public static final PrimitiveLongCodec BIGINT = new BigIntCodec();
  public static final PrimitiveShortCodec SMALLINT = new SmallIntCodec();

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * Instant}, using the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information. If you need a different time zone, consider other constants in
   * this class, or call {@link #timestampAt(ZoneId)} instead.
   *
   * @see #TIMESTAMP_UTC
   * @see #timestampAt(ZoneId)
   */
  public static final TypeCodec<Instant> TIMESTAMP = new TimestampCodec();

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * Instant}, using {@link ZoneOffset#UTC} as its source of time zone information. If you need a
   * different time zone, consider other constants in this class, or call {@link
   * #timestampAt(ZoneId)} instead.
   *
   * @see #TIMESTAMP
   * @see #timestampAt(ZoneId)
   */
  public static final TypeCodec<Instant> TIMESTAMP_UTC = new TimestampCodec(ZoneOffset.UTC);

  /**
   * A codec that maps CQL {@code timestamp} to Java {@code long}, representing the number of
   * milliseconds since the Epoch, using the system's {@linkplain ZoneId#systemDefault() default
   * time zone} as its source of time zone information. If you need a different time zone, consider
   * other constants in this class, or call {@link #timestampMillisAt(ZoneId)} instead.
   *
   * <p>This codec can serve as a replacement for the driver's built-in {@linkplain #TIMESTAMP
   * timestamp} codec, when application code prefers to deal with raw milliseconds than with {@link
   * Instant} instances.
   *
   * @see #TIMESTAMP_MILLIS_UTC
   * @see #timestampMillisAt(ZoneId)
   */
  public static final PrimitiveLongCodec TIMESTAMP_MILLIS_SYSTEM = new TimestampMillisCodec();

  /**
   * A codec that maps CQL {@code timestamp} to Java {@code long}, representing the number of
   * milliseconds since the Epoch, using {@link ZoneOffset#UTC} as its source of time zone
   * information. If you need a different time zone, consider other constants in this class, or call
   * {@link #timestampMillisAt(ZoneId)} instead.
   *
   * <p>This codec can serve as a replacement for the driver's built-in {@linkplain #TIMESTAMP
   * timestamp} codec, when application code prefers to deal with raw milliseconds than with {@link
   * Instant} instances.
   *
   * @see #TIMESTAMP_MILLIS_SYSTEM
   * @see #timestampMillisAt(ZoneId)
   */
  public static final PrimitiveLongCodec TIMESTAMP_MILLIS_UTC =
      new TimestampMillisCodec(ZoneOffset.UTC);

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * ZonedDateTime}, using the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information. If you need a different time zone, consider using other
   * constants in this class, or call {@link #zonedTimestampAt(ZoneId)} instead.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with zoned timestamps in their
   * applications.
   *
   * @see #ZONED_TIMESTAMP_UTC
   * @see #ZONED_TIMESTAMP_PERSISTED
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_SYSTEM = new ZonedTimestampCodec();

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * ZonedDateTime}, using {@link ZoneOffset#UTC} as its source of time zone information. If you
   * need a different time zone, consider using other constants in this class, or call {@link
   * #zonedTimestampAt(ZoneId)} instead.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with zoned timestamps in their
   * applications.
   *
   * @see #ZONED_TIMESTAMP_SYSTEM
   * @see #ZONED_TIMESTAMP_PERSISTED
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_UTC =
      new ZonedTimestampCodec(ZoneOffset.UTC);

  /**
   * A codec that maps {@link ZonedDateTime} to CQL {@code tuple<timestamp,varchar>}, providing a
   * pattern for maintaining timezone information in Cassandra.
   *
   * <p>Since Cassandra's <code>timestamp</code> type does not store any time zone, by using a
   * <code>tuple&lt;timestamp,varchar&gt;</code> a timezone can be persisted in the <code>varchar
   * </code> field of such tuples, and so when the value is deserialized the original timezone is
   * preserved.
   *
   * @see #ZONED_TIMESTAMP_SYSTEM
   * @see #ZONED_TIMESTAMP_UTC
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_PERSISTED =
      new PersistentZonedTimestampCodec();

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * LocalDateTime}, using the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information. If you need a different time zone, consider using other
   * constants in this class, or call {@link #localTimestampAt(ZoneId)} instead.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with local date-times in their
   * applications.
   *
   * @see #LOCAL_TIMESTAMP_UTC
   * @see #localTimestampAt(ZoneId)
   */
  public static final TypeCodec<LocalDateTime> LOCAL_TIMESTAMP_SYSTEM = new LocalTimestampCodec();

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * LocalDateTime}, using {@link ZoneOffset#UTC} as its source of time zone information.If you need
   * a different time zone, consider using other constants in this class, or call {@link
   * #localTimestampAt(ZoneId)} instead.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with local date-times in their
   * applications.
   *
   * @see #LOCAL_TIMESTAMP_SYSTEM
   * @see #localTimestampAt(ZoneId)
   */
  public static final TypeCodec<LocalDateTime> LOCAL_TIMESTAMP_UTC =
      new LocalTimestampCodec(ZoneOffset.UTC);

  public static final TypeCodec<LocalDate> DATE = new DateCodec();
  public static final TypeCodec<LocalTime> TIME = new TimeCodec();

  /**
   * A codec that maps the CQL type {@code blob} to the Java type {@link ByteBuffer}.
   *
   * <p>If you are looking for a codec mapping the CQL type {@code blob} to the Java type {@code
   * byte[]}, you should use {@link #BLOB_SIMPLE} instead.
   *
   * <p>If you are looking for a codec mapping the CQL type {@code list<tinyint} to the Java type
   * {@code byte[]}, you should use {@link #BYTE_ARRAY} instead.
   *
   * @see #BLOB_SIMPLE
   * @see #BYTE_ARRAY
   */
  public static final TypeCodec<ByteBuffer> BLOB = new BlobCodec();

  /**
   * A codec that maps the CQL type {@code blob} to the Java type {@code byte[]}.
   *
   * <p>If you are looking for a codec mapping the CQL type {@code blob} to the Java type {@link
   * ByteBuffer}, you should use {@link #BLOB} instead.
   *
   * <p>If you are looking for a codec mapping the CQL type {@code list<tinyint} to the Java type
   * {@code byte[]}, you should use {@link #BYTE_ARRAY} instead.
   *
   * @see #BLOB
   * @see #BYTE_ARRAY
   */
  public static final TypeCodec<byte[]> BLOB_SIMPLE = new SimpleBlobCodec();

  public static final TypeCodec<String> TEXT = new StringCodec(DataTypes.TEXT, Charsets.UTF_8);
  public static final TypeCodec<String> ASCII = new StringCodec(DataTypes.ASCII, Charsets.US_ASCII);
  public static final TypeCodec<BigInteger> VARINT = new VarIntCodec();
  public static final TypeCodec<BigDecimal> DECIMAL = new DecimalCodec();
  public static final TypeCodec<UUID> UUID = new UuidCodec();
  public static final TypeCodec<UUID> TIMEUUID = new TimeUuidCodec();
  public static final TypeCodec<InetAddress> INET = new InetCodec();
  public static final TypeCodec<CqlDuration> DURATION = new CqlDurationCodec();

  /** A codec that maps CQL {@code list<boolean>} to Java {@code boolean[]}. */
  public static final TypeCodec<boolean[]> BOOLEAN_ARRAY = new BooleanArrayCodec();

  /**
   * A codec that maps CQL {@code list<tinyint>} to Java {@code byte[]}.
   *
   * <p>Note that this codec is not suitable for reading CQL blobs as byte arrays. If you are
   * looking for a codec for the CQL type {@code blob}, you should use {@link #BLOB} or {@link
   * #BLOB_SIMPLE} instead.
   *
   * @see #BLOB
   * @see #BLOB_SIMPLE
   */
  public static final TypeCodec<byte[]> BYTE_ARRAY = new ByteArrayCodec();

  /** A codec that maps CQL {@code list<smallint>} to Java {@code short[]}. */
  public static final TypeCodec<short[]> SHORT_ARRAY = new ShortArrayCodec();

  /** A codec that maps CQL {@code list<int>} to Java {@code int[]}. */
  public static final TypeCodec<int[]> INT_ARRAY = new IntArrayCodec();

  /** A codec that maps CQL {@code list<bigint>} to Java {@code long[]}. */
  public static final TypeCodec<long[]> LONG_ARRAY = new LongArrayCodec();

  /** A codec that maps CQL {@code list<float>} to Java {@code float[]}. */
  public static final TypeCodec<float[]> FLOAT_ARRAY = new FloatArrayCodec();

  /** A codec that maps CQL {@code list<double>} to Java {@code double[]}. */
  public static final TypeCodec<double[]> DOUBLE_ARRAY = new DoubleArrayCodec();

  @NonNull
  public static TypeCodec<ByteBuffer> custom(@NonNull DataType cqlType) {
    Preconditions.checkArgument(cqlType instanceof CustomType, "cqlType must be a custom type");
    return new CustomCodec((CustomType) cqlType);
  }

  @NonNull
  public static <T> TypeCodec<List<T>> listOf(@NonNull TypeCodec<T> elementCodec) {
    return new ListCodec<>(DataTypes.listOf(elementCodec.getCqlType()), elementCodec);
  }

  @NonNull
  public static <T> TypeCodec<Set<T>> setOf(@NonNull TypeCodec<T> elementCodec) {
    return new SetCodec<>(DataTypes.setOf(elementCodec.getCqlType()), elementCodec);
  }

  @NonNull
  public static <K, V> TypeCodec<Map<K, V>> mapOf(
      @NonNull TypeCodec<K> keyCodec, @NonNull TypeCodec<V> valueCodec) {
    return new MapCodec<>(
        DataTypes.mapOf(keyCodec.getCqlType(), valueCodec.getCqlType()), keyCodec, valueCodec);
  }

  @NonNull
  public static TypeCodec<TupleValue> tupleOf(@NonNull TupleType cqlType) {
    return new TupleCodec(cqlType);
  }

  @NonNull
  public static TypeCodec<UdtValue> udtOf(@NonNull UserDefinedType cqlType) {
    return new UdtCodec(cqlType);
  }

  /**
   * Returns a codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * Instant}, using the supplied {@link ZoneId} as its source of time zone information.
   *
   * @see #TIMESTAMP
   * @see #TIMESTAMP_UTC
   */
  @NonNull
  public static TypeCodec<Instant> timestampAt(@NonNull ZoneId timeZone) {
    return new TimestampCodec(timeZone);
  }

  /**
   * A codec that maps CQL {@code timestamp} to Java {@code long}, representing the number of
   * milliseconds since the Epoch, using the supplied {@link ZoneId} as its source of time zone
   * information.
   *
   * <p>This codec can serve as a replacement for the driver's built-in {@linkplain #TIMESTAMP
   * timestamp} codec, when application code prefers to deal with raw milliseconds than with {@link
   * Instant} instances.
   *
   * @see #TIMESTAMP_MILLIS_SYSTEM
   * @see #TIMESTAMP_MILLIS_UTC
   */
  @NonNull
  public static PrimitiveLongCodec timestampMillisAt(@NonNull ZoneId timeZone) {
    return new TimestampMillisCodec(timeZone);
  }

  /**
   * Returns a codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * ZonedDateTime}, using the supplied {@link ZoneId} as its source of time zone information.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; the codecs
   * created by this method are provided merely as a convenience for users that need to deal with
   * zoned timestamps in their applications.
   *
   * @see #ZONED_TIMESTAMP_SYSTEM
   * @see #ZONED_TIMESTAMP_UTC
   * @see #ZONED_TIMESTAMP_PERSISTED
   */
  @NonNull
  public static TypeCodec<ZonedDateTime> zonedTimestampAt(@NonNull ZoneId timeZone) {
    return new ZonedTimestampCodec(timeZone);
  }

  /**
   * Returns a codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * LocalDateTime}, using the supplied {@link ZoneId} as its source of time zone information.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; the codecs
   * created by this method are provided merely as a convenience for users that need to deal with
   * local date-times in their applications.
   *
   * @see #LOCAL_TIMESTAMP_UTC
   * @see #localTimestampAt(ZoneId)
   */
  @NonNull
  public static TypeCodec<LocalDateTime> localTimestampAt(@NonNull ZoneId timeZone) {
    return new LocalTimestampCodec(timeZone);
  }

  /**
   * Returns a codec mapping CQL lists to Java object arrays. Serialization and deserialization of
   * elements in the array is delegated to the provided element codec.
   *
   * <p>This method is not suitable for Java primitive arrays. Use {@link #BOOLEAN_ARRAY}, {@link
   * #BYTE_ARRAY}, {@link #SHORT_ARRAY}, {@link #INT_ARRAY}, {@link #LONG_ARRAY}, {@link
   * #FLOAT_ARRAY} or {@link #DOUBLE_ARRAY} instead.
   */
  @NonNull
  public static <T> TypeCodec<T[]> arrayOf(@NonNull TypeCodec<T> elementCodec) {
    return new ObjectArrayCodec<>(elementCodec);
  }

  /**
   * Returns a codec mapping Java Enums to CQL ints, according to their {@linkplain Enum#ordinal()
   * ordinals}.
   *
   * @see #enumNamesOf(Class)
   */
  @NonNull
  public static <EnumT extends Enum<EnumT>> TypeCodec<EnumT> enumOrdinalsOf(
      @NonNull Class<EnumT> enumClass) {
    return new EnumOrdinalCodec<>(enumClass);
  }

  /**
   * Returns a codec mapping Java Enums to CQL varchars, according to their programmatic {@linkplain
   * Enum#name() names}.
   *
   * @see #enumOrdinalsOf(Class)
   */
  @NonNull
  public static <EnumT extends Enum<EnumT>> TypeCodec<EnumT> enumNamesOf(
      @NonNull Class<EnumT> enumClass) {
    return new EnumNameCodec<>(enumClass);
  }

  /** Returns a codec wrapping another codec's Java type into {@link Optional} instances. */
  @NonNull
  public static <T> TypeCodec<Optional<T>> optionalOf(@NonNull TypeCodec<T> innerCodec) {
    return new OptionalCodec<>(innerCodec);
  }

  /**
   * Returns a codec that maps the given Java type to JSON strings, using a default Jackson JSON
   * mapper to perform serialization and deserialization of JSON objects.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(@NonNull GenericType<T> javaType) {
    return new JsonCodec<>(javaType);
  }

  /**
   * Returns a codec that maps the given Java class to JSON strings, using a default Jackson JSON
   * mapper to perform serialization and deserialization of JSON objects.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(@NonNull Class<T> javaType) {
    return new JsonCodec<>(javaType);
  }

  /**
   * Returns a codec that maps the given Java type to JSON strings, using the provided Jackson
   * {@link JsonMapper} to perform serialization and deserialization of JSON objects.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(
      @NonNull GenericType<T> javaType, @NonNull JsonMapper jsonMapper) {
    return new JsonCodec<>(javaType, jsonMapper);
  }

  /**
   * Returns a codec that maps the given Java class to JSON strings, using the provided Jackson
   * {@link JsonMapper} to perform serialization and deserialization of JSON objects.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(@NonNull Class<T> javaType, @NonNull JsonMapper jsonMapper) {
    return new JsonCodec<>(javaType, jsonMapper);
  }
}
