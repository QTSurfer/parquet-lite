package com.wualabs.qtsurfer.parquet;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ParquetCompressionCodecTest {

  private static final int ROWS = 1000;

  private static final MessageType SCHEMA =
      new MessageType(
          "ticker", Types.required(INT64).named("t"), Types.required(DOUBLE).named("cls"));

  private static final Dehydrator<long[]> DEHYDRATOR =
      (record, valueWriter) -> {
        valueWriter.write("t", record[0]);
        valueWriter.write("cls", Double.longBitsToDouble(record[1]));
      };

  private static final Hydrator<Map<String, Object>, Map<String, Object>> HYDRATOR =
      new Hydrator<>() {
        @Override
        public Map<String, Object> start() {
          return new HashMap<>();
        }

        @Override
        public Map<String, Object> add(Map<String, Object> target, String heading, Object value) {
          HashMap<String, Object> r = new HashMap<>(target);
          r.put(heading, value);
          return r;
        }

        @Override
        public Map<String, Object> finish(Map<String, Object> target) {
          return target;
        }
      };

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void writeFile_defaultCodec_isSnappy() throws IOException {
    File file = new File(folder.getRoot(), "default.parquet");
    writeData(ParquetWriter.writeFile(SCHEMA, file, DEHYDRATOR));

    File zstd = new File(folder.getRoot(), "zstd.parquet");
    writeData(ParquetWriter.writeFile(SCHEMA, zstd, DEHYDRATOR, CompressionCodecName.ZSTD));

    // Default (SNAPPY) and ZSTD should both produce valid files with different sizes
    assertTrue("default file should have data", file.length() > 0);
    assertTrue("zstd file should have data", zstd.length() > 0);
    assertEquals(ROWS, readRowCount(file));
    assertEquals(ROWS, readRowCount(zstd));
  }

  @Test
  public void zstd_produces_smaller_files_than_uncompressed() throws IOException {
    File zstd = new File(folder.getRoot(), "zstd.parquet");
    writeData(ParquetWriter.writeFile(SCHEMA, zstd, DEHYDRATOR, CompressionCodecName.ZSTD));

    File raw = new File(folder.getRoot(), "raw.parquet");
    writeData(
        ParquetWriter.writeFile(SCHEMA, raw, DEHYDRATOR, CompressionCodecName.UNCOMPRESSED));

    assertTrue("ZSTD should be smaller than UNCOMPRESSED", zstd.length() < raw.length());
  }

  @Test
  public void snappy_produces_smaller_files_than_uncompressed() throws IOException {
    File snappy = new File(folder.getRoot(), "snappy.parquet");
    writeData(ParquetWriter.writeFile(SCHEMA, snappy, DEHYDRATOR, CompressionCodecName.SNAPPY));

    File raw = new File(folder.getRoot(), "raw.parquet");
    writeData(
        ParquetWriter.writeFile(SCHEMA, raw, DEHYDRATOR, CompressionCodecName.UNCOMPRESSED));

    assertTrue("SNAPPY should be smaller than UNCOMPRESSED", snappy.length() < raw.length());
  }

  @Test
  public void zstd_data_reads_back_correctly() throws IOException {
    File file = new File(folder.getRoot(), "zstd.parquet");
    writeData(ParquetWriter.writeFile(SCHEMA, file, DEHYDRATOR, CompressionCodecName.ZSTD));

    try (Stream<Map<String, Object>> s =
        ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
      List<Map<String, Object>> rows = s.collect(Collectors.toList());
      assertEquals(ROWS, rows.size());
      assertEquals(1000000L, rows.get(0).get("t"));
      assertEquals(1000000L + (ROWS - 1) * 1000L, rows.get(ROWS - 1).get("t"));
    }
  }

  @Test
  public void writeOutputStream_with_codec() throws IOException {
    File file = new File(folder.getRoot(), "stream_zstd.parquet");
    try (FileOutputStream fos = new FileOutputStream(file)) {
      writeData(
          ParquetWriter.writeOutputStream(SCHEMA, fos, DEHYDRATOR, CompressionCodecName.ZSTD));
    }

    assertTrue("stream ZSTD file should have data", file.length() > 0);
    assertEquals(ROWS, readRowCount(file));
  }

  @Test
  public void all_codecs_produce_valid_output() throws IOException {
    // GZIP and LZ4 require hadoop-common (GzipCodec/Lz4Codec), not available without Hadoop
    for (CompressionCodecName codec :
        List.of(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.ZSTD)) {
      File file = new File(folder.getRoot(), codec.name() + ".parquet");
      writeData(ParquetWriter.writeFile(SCHEMA, file, DEHYDRATOR, codec));
      assertEquals(codec.name() + " row count", ROWS, readRowCount(file));
    }
  }

  private void writeData(ParquetWriter<long[]> writer) throws IOException {
    try (writer) {
      long baseTs = 1000000L;
      for (int i = 0; i < ROWS; i++) {
        long ts = baseTs + i * 1000L;
        double cls = 84000.0 + Math.sin(i * 0.01) * 500;
        writer.write(new long[] {ts, Double.doubleToLongBits(cls)});
      }
    }
  }

  private int readRowCount(File file) throws IOException {
    try (Stream<Map<String, Object>> s =
        ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
      return (int) s.count();
    }
  }
}
