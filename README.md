# parquet-lite

[![CI](https://github.com/QTSurfer/parquet-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/QTSurfer/parquet-lite/actions/workflows/ci.yml)
[![JitPack](https://jitpack.io/v/QTSurfer/parquet-lite.svg)](https://jitpack.io/#QTSurfer/parquet-lite)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Lightweight Java library for reading and writing Apache Parquet files without Hadoop dependencies.

Fork of [strategicblue/parquet-floor](https://github.com/strategicblue/parquet-floor).

## Features

- Read and write Parquet files with a simple API
- No Hadoop dependency tree — minimal stubs included
- Write to `File`, `OutputStream`, or any `OutputFile` implementation
- Configurable compression codecs

### Supported types

| Primitive | Logical Type | Java Type | Notes |
|-----------|-------------|-----------|-------|
| INT32 | — | `int` | |
| INT64 | — | `long` | |
| INT64 | TIMESTAMP(NANOS, UTC) | `long` | Nanos since epoch, compatible with QuestDB `now_ns()` |
| FLOAT | — | `float` | |
| DOUBLE | — | `double` | |
| BOOLEAN | — | `boolean` | |
| BINARY | STRING | `String` | |
| BINARY | JSON | `String` | |
| BINARY | ENUM | `String` | |
| FIXED_LEN_BYTE_ARRAY(16) | UUID | `java.util.UUID` | |
| FIXED_LEN_BYTE_ARRAY | DECIMAL | `java.math.BigDecimal` | Configurable precision/scale |

### Supported compression codecs

| Codec | Status | Library |
|-------|--------|---------|
| UNCOMPRESSED | Supported | Built-in |
| SNAPPY | Supported | xerial-snappy (no Hadoop) |
| ZSTD | Supported | zstd-jni (no Hadoop) |
| GZIP | Not supported | Requires hadoop-common |
| LZ4 | Not supported | Requires hadoop-common |

## Usage

### Writing

```java
MessageType schema = new MessageType("ticker",
    Types.required(PrimitiveTypeName.INT64).named("t"),
    Types.required(PrimitiveTypeName.DOUBLE).named("cls"));

Dehydrator<Tick> dehydrator = (tick, writer) -> {
    writer.write("t", tick.timestamp());
    writer.write("cls", tick.close());
};

// Default codec (SNAPPY)
try (ParquetWriter<Tick> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
    writer.write(tick);
}

// Explicit codec
try (ParquetWriter<Tick> writer = ParquetWriter.writeFile(schema, file, dehydrator,
        CompressionCodecName.ZSTD)) {
    writer.write(tick);
}

// Write to OutputStream
try (ParquetWriter<Tick> writer = ParquetWriter.writeOutputStream(schema, outputStream,
        dehydrator, CompressionCodecName.ZSTD)) {
    writer.write(tick);
}
```

### Writing with extended types

```java
MessageType schema = new MessageType("trades",
    Types.required(INT64)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
        .named("ts_ns"),
    Types.required(FIXED_LEN_BYTE_ARRAY).length(16)
        .as(LogicalTypeAnnotation.uuidType()).named("trade_id"),
    Types.required(FIXED_LEN_BYTE_ARRAY).length(16)
        .as(LogicalTypeAnnotation.decimalType(2, 18)).named("price"),
    Types.required(BINARY)
        .as(LogicalTypeAnnotation.enumType()).named("exchange"));

Dehydrator<Trade> dehydrator = (trade, writer) -> {
    writer.write("ts_ns", trade.timestampNanos());
    writer.write("trade_id", trade.id());        // UUID
    writer.write("price", trade.price());         // BigDecimal
    writer.write("exchange", trade.exchange());   // String
};
```

### Reading

```java
Hydrator<Map<String, Object>, Map<String, Object>> hydrator = new Hydrator<>() {
    public Map<String, Object> start() { return new HashMap<>(); }
    public Map<String, Object> add(Map<String, Object> target, String heading, Object value) {
        target.put(heading, value);
        return target;
    }
    public Map<String, Object> finish(Map<String, Object> target) { return target; }
};

try (Stream<Map<String, Object>> rows = ParquetReader.streamContent(file,
        HydratorSupplier.constantly(hydrator))) {
    rows.forEach(row -> System.out.println(row));
}
```

## Dependency (JitPack)

### Maven

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.qtsurfer</groupId>
    <artifactId>parquet-lite</artifactId>
    <version>2.0.0</version>
</dependency>
```

### Gradle

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.qtsurfer:parquet-lite:2.0.0'
}
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Attribution

- Original work: Copyright Strategic Blue Ltd — [strategicblue/parquet-floor](https://github.com/strategicblue/parquet-floor)
- Fork maintenance: Copyright Wualabs LTD — [wualabs.com](https://wualabs.com)

See [NOTICE](NOTICE) for details.
