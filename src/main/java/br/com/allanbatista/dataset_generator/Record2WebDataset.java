package br.com.allanbatista.dataset_generator;

import org.apache.beam.sdk.transforms.DoFn;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class Record2WebDataset extends DoFn<Record, byte[]> {
    @ProcessElement
    public void processElement(@Element Record data, OutputReceiver<byte[]> out) throws Exception {

        Map<String, byte[]> output = new HashMap<String, byte[]>();
        output.put("image", data.image.getBytes());
        output.put("record", data.toJson().getBytes());

        // Convert Map to byte array
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(byteOut);
        outputStream.writeObject(output);

        out.output(byteOut.toByteArray());
    }
}
