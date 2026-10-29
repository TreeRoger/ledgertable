package dev.ledgertable.io;

import dev.ledgertable.core.UsageEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Encodes usage events to and from Parquet bytes, so that object stores only ever see byte arrays. */
public final class ParquetCodec {
    public static final MessageType SCHEMA = MessageTypeParser.parseMessageType("""
            message usage_event {
              required binary event_id (STRING);
              required binary account_id (STRING);
              required binary resource_id (STRING);
              required int64 compute_millis;
              required int64 event_time (TIMESTAMP(MILLIS,true));
            }""");

    private ParquetCodec() {
    }

    public static byte[] encode(List<UsageEvent> events) {
        var sink = new ByteArrayOutputStream();
        var factory = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new MemoryOutputFile(sink))
                .withConf(new Configuration(false))
                .withType(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (UsageEvent e : events) {
                writer.write(factory.newGroup()
                        .append("event_id", e.eventId())
                        .append("account_id", e.accountId())
                        .append("resource_id", e.resourceId())
                        .append("compute_millis", e.computeMillis())
                        .append("event_time", e.eventTimeMillis()));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return sink.toByteArray();
    }

    public static List<UsageEvent> decode(byte[] bytes) {
        var events = new ArrayList<UsageEvent>();
        try (ParquetFileReader reader = ParquetFileReader.open(new MemoryInputFile(bytes))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            var columns = new ColumnIOFactory().getColumnIO(schema);
            for (PageReadStore rowGroup = reader.readNextRowGroup(); rowGroup != null; rowGroup = reader.readNextRowGroup()) {
                RecordReader<Group> records = columns.getRecordReader(rowGroup, new GroupRecordConverter(schema));
                for (long i = 0; i < rowGroup.getRowCount(); i++) {
                    Group g = records.read();
                    events.add(new UsageEvent(
                            g.getString("event_id", 0),
                            g.getString("account_id", 0),
                            g.getString("resource_id", 0),
                            g.getLong("compute_millis", 0),
                            g.getLong("event_time", 0)));
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return events;
    }

    private record MemoryOutputFile(ByteArrayOutputStream sink) implements OutputFile {
        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return createOrOverwrite(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            sink.reset();
            return new DelegatingPositionOutputStream(sink) {
                @Override
                public long getPos() {
                    return sink.size();
                }
            };
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private record MemoryInputFile(byte[] bytes) implements InputFile {
        @Override
        public long getLength() {
            return bytes.length;
        }

        @Override
        public SeekableInputStream newStream() {
            var in = new ByteArrayInputStream(bytes);
            return new DelegatingSeekableInputStream(in) {
                @Override
                public long getPos() {
                    return bytes.length - in.available();
                }

                @Override
                public void seek(long newPos) {
                    in.reset();
                    in.skip(newPos);
                }
            };
        }
    }
}
