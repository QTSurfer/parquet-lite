package com.wualabs.qtsurfer.parquet;

import com.wualabs.qtsurfer.parquet.io.FileParquetOutput;
import com.wualabs.qtsurfer.parquet.io.StreamParquetOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

public final class ParquetWriter<T> implements Closeable {

    private final org.apache.parquet.hadoop.ParquetWriter<T> writer;

    public static <T> ParquetWriter<T> writeOutputStream(MessageType schema, OutputStream out, Dehydrator<T> dehydrator) throws IOException {
        return writeOutput(schema, new StreamParquetOutput(out), dehydrator, CompressionCodecName.SNAPPY);
    }

    public static <T> ParquetWriter<T> writeOutputStream(MessageType schema, OutputStream out,
            Dehydrator<T> dehydrator, CompressionCodecName codec) throws IOException {
        return writeOutput(schema, new StreamParquetOutput(out), dehydrator, codec);
    }

    public static <T> ParquetWriter<T> writeFile(MessageType schema, File out, Dehydrator<T> dehydrator) throws IOException {
        return writeOutput(schema, new FileParquetOutput(out), dehydrator, CompressionCodecName.SNAPPY);
    }

    public static <T> ParquetWriter<T> writeFile(MessageType schema, File out, Dehydrator<T> dehydrator, CompressionCodecName codec) throws IOException {
        return writeOutput(schema, new FileParquetOutput(out), dehydrator, codec);
    }

    public static <T> ParquetWriter<T> writeOutput(MessageType schema, OutputFile file, Dehydrator<T> dehydrator) throws IOException {
        return new ParquetWriter<>(file, schema, dehydrator, CompressionCodecName.SNAPPY);
    }

    public static <T> ParquetWriter<T> writeOutput(MessageType schema, OutputFile file,
            Dehydrator<T> dehydrator, CompressionCodecName codec) throws IOException {
        return new ParquetWriter<>(file, schema, dehydrator, codec);
    }

    private ParquetWriter(OutputFile outputFile, MessageType schema, Dehydrator<T> dehydrator, CompressionCodecName codec) throws IOException {
        this.writer = new Builder<T>(outputFile)
                .withType(schema)
                .withDehydrator(dehydrator)
                .withCompressionCodec(codec)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .build();
    }

    public void write(T record) throws IOException {
        writer.write(record);
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    private static final class Builder<T> extends org.apache.parquet.hadoop.ParquetWriter.Builder<T, ParquetWriter.Builder<T>> {
        private MessageType schema;
        private Dehydrator<T> dehydrator;

        private Builder(OutputFile file) {
            super(file);
        }

        public ParquetWriter.Builder<T> withType(MessageType schema) {
            this.schema = schema;
            return this;
        }

        public ParquetWriter.Builder<T> withDehydrator(Dehydrator<T> dehydrator) {
            this.dehydrator = dehydrator;
            return this;
        }

        @Override
        protected ParquetWriter.Builder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return new SimpleWriteSupport<>(schema, dehydrator);
        }
    }

    private static class SimpleWriteSupport<T> extends WriteSupport<T> {
        private final MessageType schema;
        private final Dehydrator<T> dehydrator;
        private final ValueWriter valueWriter = SimpleWriteSupport.this::writeField;

        private RecordConsumer recordConsumer;

        SimpleWriteSupport(MessageType schema, Dehydrator<T> dehydrator) {
            this.schema = schema;
            this.dehydrator = dehydrator;
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, Collections.emptyMap());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(T record) {
            recordConsumer.startMessage();
            dehydrator.dehydrate(record, valueWriter);
            recordConsumer.endMessage();
        }

        @Override
        public String getName() {
            return "com.wualabs.qtsurfer.parquet.ParquetWriter";
        }

        private void writeField(String name, Object value) {
            int fieldIndex = schema.getFieldIndex(name);
            PrimitiveType type = schema.getType(fieldIndex).asPrimitiveType();
            recordConsumer.startField(name, fieldIndex);

            switch (type.getPrimitiveTypeName()) {
            case INT32: recordConsumer.addInteger((int) value); break;
            case INT64: recordConsumer.addLong((long) value); break;
            case DOUBLE: recordConsumer.addDouble((double) value); break;
            case BOOLEAN: recordConsumer.addBoolean((boolean) value); break;
            case FLOAT: recordConsumer.addFloat((float) value); break;
            case BINARY:
                LogicalTypeAnnotation binaryAnnotation = type.getLogicalTypeAnnotation();
                if (binaryAnnotation == LogicalTypeAnnotation.stringType()
                    || binaryAnnotation == LogicalTypeAnnotation.jsonType()
                    || binaryAnnotation == LogicalTypeAnnotation.enumType()) {
                    recordConsumer.addBinary(Binary.fromString((String) value));
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BINARY logical type: " + binaryAnnotation);
                }
                break;
            case FIXED_LEN_BYTE_ARRAY:
                LogicalTypeAnnotation fixedAnnotation = type.getLogicalTypeAnnotation();
                if (fixedAnnotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
                    UUID uuid = (UUID) value;
                    ByteBuffer buf = ByteBuffer.allocate(16);
                    buf.putLong(uuid.getMostSignificantBits());
                    buf.putLong(uuid.getLeastSignificantBits());
                    recordConsumer.addBinary(Binary.fromConstantByteArray(buf.array()));
                } else if (fixedAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    BigDecimal decimal = (BigDecimal) value;
                    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decAnnotation =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) fixedAnnotation;
                    BigDecimal scaled = decimal.setScale(decAnnotation.getScale());
                    byte[] unscaled = scaled.unscaledValue().toByteArray();
                    int len = type.getTypeLength();
                    byte[] padded = new byte[len];
                    if (scaled.signum() < 0) {
                        java.util.Arrays.fill(padded, (byte) 0xFF);
                    }
                    System.arraycopy(unscaled, 0, padded, len - unscaled.length, unscaled.length);
                    recordConsumer.addBinary(Binary.fromConstantByteArray(padded));
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported FIXED_LEN_BYTE_ARRAY logical type: " + fixedAnnotation);
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported primitive type: " + type.getPrimitiveTypeName());
            }
            recordConsumer.endField(name, fieldIndex);
        }
    }
}
