package com.wualabs.qtsurfer.parquet;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ParquetExtendedTypesTest {

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

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void uuid_roundtrip() throws IOException {
        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.randomUUID();

        MessageType schema = new MessageType("test",
                Types.required(FIXED_LEN_BYTE_ARRAY).length(16)
                        .as(LogicalTypeAnnotation.uuidType()).named("id"),
                Types.required(INT64).named("ts"));

        Dehydrator<Object[]> dehydrator = (record, writer) -> {
            writer.write("id", record[0]);
            writer.write("ts", record[1]);
        };

        File file = new File(folder.getRoot(), "uuid.parquet");
        try (ParquetWriter<Object[]> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
            writer.write(new Object[]{id1, 1000L});
            writer.write(new Object[]{id2, 2000L});
        }

        try (Stream<Map<String, Object>> s =
                ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
            List<Map<String, Object>> rows = s.collect(Collectors.toList());
            assertEquals(2, rows.size());
            assertEquals(id1, rows.get(0).get("id"));
            assertEquals(id2, rows.get(1).get("id"));
            assertEquals(1000L, rows.get(0).get("ts"));
        }
    }

    @Test
    public void decimal_roundtrip() throws IOException {
        BigDecimal price1 = new BigDecimal("84123.45");
        BigDecimal price2 = new BigDecimal("0.01");
        BigDecimal price3 = new BigDecimal("-500.99");

        MessageType schema = new MessageType("test",
                Types.required(FIXED_LEN_BYTE_ARRAY).length(16)
                        .as(LogicalTypeAnnotation.decimalType(2, 18)).named("price"),
                Types.required(INT64).named("ts"));

        Dehydrator<Object[]> dehydrator = (record, writer) -> {
            writer.write("price", record[0]);
            writer.write("ts", record[1]);
        };

        File file = new File(folder.getRoot(), "decimal.parquet");
        try (ParquetWriter<Object[]> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
            writer.write(new Object[]{price1, 1000L});
            writer.write(new Object[]{price2, 2000L});
            writer.write(new Object[]{price3, 3000L});
        }

        try (Stream<Map<String, Object>> s =
                ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
            List<Map<String, Object>> rows = s.collect(Collectors.toList());
            assertEquals(3, rows.size());
            assertEquals(0, price1.compareTo((BigDecimal) rows.get(0).get("price")));
            assertEquals(0, price2.compareTo((BigDecimal) rows.get(1).get("price")));
            assertEquals(0, price3.compareTo((BigDecimal) rows.get(2).get("price")));
        }
    }

    @Test
    public void timestamp_nanos_roundtrip() throws IOException {
        long tsNanos1 = 1712188800000000000L; // 2024-04-04T00:00:00Z in nanos
        long tsNanos2 = 1712188800123456789L; // with sub-second nanos

        MessageType schema = new MessageType("test",
                Types.required(INT64)
                        .as(LogicalTypeAnnotation.timestampType(true,
                                LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("ts_ns"),
                Types.required(INT64).named("value"));

        Dehydrator<Object[]> dehydrator = (record, writer) -> {
            writer.write("ts_ns", record[0]);
            writer.write("value", record[1]);
        };

        File file = new File(folder.getRoot(), "ts_nanos.parquet");
        try (ParquetWriter<Object[]> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
            writer.write(new Object[]{tsNanos1, 100L});
            writer.write(new Object[]{tsNanos2, 200L});
        }

        try (Stream<Map<String, Object>> s =
                ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
            List<Map<String, Object>> rows = s.collect(Collectors.toList());
            assertEquals(2, rows.size());
            assertEquals(tsNanos1, rows.get(0).get("ts_ns"));
            assertEquals(tsNanos2, rows.get(1).get("ts_ns"));
        }
    }

    @Test
    public void enum_roundtrip() throws IOException {
        MessageType schema = new MessageType("test",
                Types.required(BINARY)
                        .as(LogicalTypeAnnotation.enumType()).named("exchange"),
                Types.required(INT64).named("ts"));

        Dehydrator<Object[]> dehydrator = (record, writer) -> {
            writer.write("exchange", record[0]);
            writer.write("ts", record[1]);
        };

        File file = new File(folder.getRoot(), "enum.parquet");
        try (ParquetWriter<Object[]> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
            writer.write(new Object[]{"BINANCE", 1000L});
            writer.write(new Object[]{"BYBIT", 2000L});
        }

        try (Stream<Map<String, Object>> s =
                ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
            List<Map<String, Object>> rows = s.collect(Collectors.toList());
            assertEquals(2, rows.size());
            assertEquals("BINANCE", rows.get(0).get("exchange"));
            assertEquals("BYBIT", rows.get(1).get("exchange"));
        }
    }

    @Test
    public void date_roundtrip() throws IOException {
        LocalDate date1 = LocalDate.now();
        LocalDate date2 = LocalDate.of(2026, Month.MAY, 10);

        MessageType schema = new MessageType("test",
                Types.required(INT32)
                        .as(LogicalTypeAnnotation.dateType()).named("date"),
                Types.required(INT64).named("ts"));

        Dehydrator<Object[]> dehydrator = (record, writer) -> {
            writer.write("date", record[0]);
            writer.write("ts", record[1]);
        };

        File file = new File(folder.getRoot(), "date.parquet");
        try (ParquetWriter<Object[]> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
            writer.write(new Object[]{date1, 1000L});
            writer.write(new Object[]{date2, 2000L});
        }

        try (Stream<Map<String, Object>> s =
                     ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
            List<Map<String, Object>> rows = s.collect(Collectors.toList());
            assertEquals(2, rows.size());
            assertEquals(date1, rows.get(0).get("date"));
            assertEquals(date2, rows.get(1).get("date"));
            assertEquals(1000L, rows.get(0).get("ts"));
        }
    }

    /**
     * You can use a java.util.Date as input. However, the output will always be a LocalDate.
     */
    @Test
    public void date2localDate() throws IOException {
        Date date1 = new Date(1778371200000L);
        Date date2 = new Date(1054166400000L);

        LocalDate expectedDate1 = LocalDate.of(2026, Month.MAY, 10);
        LocalDate expectedDate2 = LocalDate.of(2003, Month.MAY, 29);

        MessageType schema = new MessageType("test",
                Types.required(INT32)
                        .as(LogicalTypeAnnotation.dateType()).named("date"),
                Types.required(INT64).named("ts"));

        Dehydrator<Object[]> dehydrator = (record, writer) -> {
            writer.write("date", record[0]);
            writer.write("ts", record[1]);
        };

        File file = new File(folder.getRoot(), "date.parquet");
        try (ParquetWriter<Object[]> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
            writer.write(new Object[]{date1, 1000L});
            writer.write(new Object[]{date2, 2000L});
        }

        try (Stream<Map<String, Object>> s =
                     ParquetReader.streamContent(file, HydratorSupplier.constantly(HYDRATOR))) {
            List<Map<String, Object>> rows = s.collect(Collectors.toList());
            assertEquals(2, rows.size());
            assertEquals(expectedDate1, rows.get(0).get("date"));
            assertEquals(expectedDate2, rows.get(1).get("date"));
            assertEquals(1000L, rows.get(0).get("ts"));
        }
    }
}
