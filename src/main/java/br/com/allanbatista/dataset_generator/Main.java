package br.com.allanbatista.dataset_generator;


import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.compress.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    static final TupleTag<Record> trainTag = new TupleTag<Record>(){};
    static final TupleTag<Record> validTag = new TupleTag<Record>(){};
    static final TupleTag<Record> testTag = new TupleTag<Record>(){};

    public interface CustomPipelineOptions extends DataflowPipelineOptions {
        @Description("Output dir to store data files")
        @Validation.Required
        String getOutputDir();
        void setOutputDir(String outputPath);

        @Description("Limit of record of query")
        @Default.Integer(0)
        Integer getLimit();
        void setLimit(Integer limit);

        @Description("Test Proportion")
        @Default.Double(0.1)
        Double getTestProportion();
        void setTestProportion(Double testProportion);

        @Description("Validation Proportion")
        @Default.Double(0.1)
        Double getValidProportion();
        void setValidProportion(Double validProportion);

        @Description("Train Shards. How much files to create")
        @Default.Integer(0)
        Integer getTrainShards();
        void setTrainShards(Integer trainShards);

        @Description("Valid Shards. How much files to create")
        @Default.Integer(0)
        Integer getValidShards();
        void setValidShards(Integer validShards);

        @Description("Test Shards. How much files to create")
        @Default.Integer(0)
        Integer getTestShards();
        void setTestShards(Integer testShards);

        @Description("Image output size")
        @Default.Integer(224)
        Integer getImageSize();
        void setImageSize(Integer imageSize);

        @Description("Image resize type: \"black\" or \"center-crop\"")
        @Default.String("black")
        String getImageResizeType();
        void setImageResizeType(String imageResizeType);

        @Description("Single Dataset")
        @Default.Boolean(false)
        Boolean getSingleDataset();
        void setSingleDataset(Boolean singleDataset);
    }

    public static class ResizeAndCropImageFn extends DoFn<Record, Record> {
        private int size;
        private String resizeType;

        public ResizeAndCropImageFn(int size, String resizeType) {
            this.size = size;
            this.resizeType = resizeType;
        }

        @ProcessElement
        public void processElement(@Element Record record, OutputReceiver<Record> out) throws Exception {
            Record result = new Record(record);

            try {
                if (resizeType.equals("center-crop")) {
                    result.image = result.image.resizeCrop(size, size);
                } else {
                    result.image = result.image.resize(size, size);
                }

                result.image = result.image.encodeToJPEG();
                out.output(result);
            } catch (ImageHelpers.ImageResizeException e) {
                System.out.println(record.toString());
                e.printStackTrace();
            }
        }
    }

    public static class ReadImageFn extends DoFn<Record, Record> {
        @ProcessElement
        public void processElement(@Element Record record, OutputReceiver<Record> out) throws IOException {
            ReadableByteChannel io = null;
            InputStream stream = null;

            try {
                Record result = new Record(record);
                io = FileSystems.open(FileSystems.matchNewResource(record.path, false));
                stream = Channels.newInputStream(io);
                result.image = new Image(IOUtils.toByteArray(stream), "jpeg");
                out.output(result);
            } catch (Exception e) {
                System.out.println(record.toString());
                e.printStackTrace();
            } finally {
                if(stream != null) { stream.close(); }
                if(io != null) { io.close(); }
            }
        }
    }

    public static class SplitDataset extends DoFn<Record, Record> {
        private double testProportion;
        private double validProportion;
        private final Random random = new Random();

        public SplitDataset(double testProportion, double validProportion) {
            this.testProportion = testProportion;
            this.validProportion = validProportion;
        }

        @ProcessElement
        public void processElement(@Element Record record, MultiOutputReceiver out) {
            double rand = this.random.nextDouble();

            if(rand < this.testProportion) {
                out.get(testTag).output(record);
            } else if (rand < this.testProportion+this.validProportion) {
                out.get(validTag).output(record);
            } else {
                out.get(trainTag).output(record);
            }
        }
    }

    public static class TableRow2Record extends DoFn<TableRow, Record> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<Record> out) {
            out.output(new Record(row));
        }
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(CustomPipelineOptions.class);
        CustomPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        String sql = "SELECT * FROM `allanbatista.openimages.training` ORDER BY RAND()";
        if ( options.getLimit() > 0 ) sql = sql + " LIMIT " + options.getLimit();
        System.out.println("============================== Query ==============================");
        System.out.println(sql);

        PCollection<Record> records =
                p.apply("Read Records from BigQuery", BigQueryIO.readTableRows().fromQuery(sql).usingStandardSql())
                    .apply("TableRow To Record", ParDo.of(new TableRow2Record()))
                    .apply("Read Images", ParDo.of(new ReadImageFn()))
                    .apply("Resize Images", ParDo.of(new ResizeAndCropImageFn(options.getImageSize(), options.getImageResizeType())));

        if (options.getSingleDataset()) {
            records.apply("Convert Records to WebDataset Format", ParDo.of(new Record2WebDataset()))
                   .apply("Write Records to WebDataset Files",
                            new WebDataset.Writer(options.getOutputDir()).withNumShards(options.getTrainShards()));
        }
        else {
            String trainPath = options.getOutputDir() + "/train";
            String validPath = options.getOutputDir() + "/valid";
            String testPath = options.getOutputDir() + "/test";

            System.out.println(
                    "trainPath=" + trainPath + "\n" +
                    "valPath=" + validPath + "\n" +
                    "testPath=" + testPath
            );

            // split dataset
            PCollectionTuple datasets = records
                    .apply("Split into test, valid and train", ParDo.of(
                            new SplitDataset(options.getTestProportion(), options.getValidProportion())
                    ).withOutputTags(trainTag, TupleTagList.of(testTag).and(validTag)));

            datasets.get(trainTag)
                    .apply("Convert Train Records to WebDataset Format", ParDo.of(new Record2WebDataset()))
                    .apply("Write Train Records to WebDataset Files",
                            new WebDataset.Writer(trainPath).withNumShards(options.getTrainShards()));

            datasets.get(validTag)
                    .apply("Convert Valid Records to WebDataset Format", ParDo.of(new Record2WebDataset()))
                    .apply("Write Valid Records to WebDataset Files",
                            new WebDataset.Writer(validPath).withNumShards(options.getValidShards()));

            datasets.get(testTag)
                    .apply("Convert Test Records to WebDataset Format", ParDo.of(new Record2WebDataset()))
                    .apply("Write Test Records to WebDataset Files",
                            new WebDataset.Writer(testPath).withNumShards(options.getTestShards()));
        }

        p.run().waitUntilFinish();
    }
}
