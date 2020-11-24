package br.com.allanbatista.dataset_generator;

import java.io.FileOutputStream;

public class Teste {
    public static void main(String[] args) throws Exception {
        String paths[] = { "6583b1ec1966c6a6", "7db5db2dd2d1850d", "9ff162e21fcba634", "7db5db2dd2d1850d-grayscale", "7db5db2dd2d1850d-cmyk" };

        for(String path : paths) {
            Image image = ImageHelpers.read_image("assets/"+path+".jpg");
            Image imageResized = image.resize(224, 224).encodeToJPEG();

            try (FileOutputStream stream = new FileOutputStream("/tmp/"+path+"-black-bars.jpg")) {
                stream.write(imageResized.getBytes());
                System.out.println("/tmp/"+path+"-black-bars.jpg");
            }

            imageResized = image.resizeCrop(224, 224).encodeToJPEG();

            try (FileOutputStream stream = new FileOutputStream("/tmp/"+path+"-center-crop.jpg")) {
                stream.write(imageResized.getBytes());
                System.out.println("/tmp/"+path+"-center-crop.jpg");
            }
        }
    }
}
