# parquet-lite

Lightweight Java library for reading and writing Apache Parquet files without Hadoop dependencies.

Fork of [strategicblue/parquet-floor](https://github.com/strategicblue/parquet-floor).

## Features

- Read and write Parquet files with a simple API
- No Hadoop dependency tree — minimal stubs included
- Write to `File`, `OutputStream`, or any `OutputFile` implementation
- Configurable compression codecs

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
- Fork maintenance: Copyright WuaLabs — [wualabs.com](https://wualabs.com)

See [NOTICE](NOTICE) for details.
