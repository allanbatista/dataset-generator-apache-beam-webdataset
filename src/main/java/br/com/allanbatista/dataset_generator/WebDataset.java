package br.com.allanbatista.dataset_generator;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.UUID;

public class WebDataset {
    public static class Writer extends PTransform<PCollection, PCollection> {
        private final String outputPath;
        private int numShards = 0;
        private Compression compression = Compression.UNCOMPRESSED;
        private String suffix = ".tar";

        public Writer(String outputPath) {
            this.outputPath = outputPath;
        }

        @Override
        public PCollection expand(PCollection input) {
            input.apply("Writing WebDataset", FileIO.<byte[]>write()
                .via(new Sink())
                .to(this.outputPath)
                .withNumShards(this.numShards)
                .withSuffix(this.suffix)
                .withCompression(this.compression)
            );

            return input;
        }

        public Writer withNumShards(int numShards){
            this.numShards = numShards;
            return this;
        }

        public Writer withCompression(Compression compression){
            this.compression = compression;
            return this;
        }

        public Writer withSuffix(String suffix){
            this.suffix = suffix;
            return this;
        }
    }

    public static class Sink implements FileIO.Sink<byte[]> {
        @Nullable
        private transient WritableByteChannel channel;
        @Nullable
        private transient OutputStream outputStream;
        @Nullable
        private transient TarArchiveOutputStream tar_channel;

        public Sink() {
        }

        public void open(WritableByteChannel channel) {
            this.channel = channel;
            this.outputStream = new BufferedOutputStream(Channels.newOutputStream(this.channel));
            this.tar_channel = new TarArchiveOutputStream(this.outputStream);
            this.tar_channel.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
        }

        public void write(byte[] element) throws IOException {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(element);
                ObjectInputStream in = new ObjectInputStream(byteIn);
                Map<String, byte[]> data2 = (Map<String, byte[]>) in.readObject();

                TarArchiveEntry entry;
                byte[] content;
                String uuid = UUID.randomUUID().toString();
                for(String key : data2.keySet()) {
                    content = data2.get(key);
                    entry = new TarArchiveEntry(uuid + "." + key);
                    entry.setSize(content.length);

                    this.tar_channel.putArchiveEntry(entry);
                    this.tar_channel.write(content);
                    this.tar_channel.closeArchiveEntry();
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        public void flush() throws IOException {
            this.tar_channel.flush();
        }
    }
}
