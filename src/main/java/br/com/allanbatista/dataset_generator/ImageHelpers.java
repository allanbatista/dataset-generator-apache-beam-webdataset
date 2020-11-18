package br.com.allanbatista.dataset_generator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ImageHelpers {
    public static class ImageFormatException extends Exception {
        public ImageFormatException(String message, Throwable exception) {
            super(message, exception);
        }
    }

    public static class ImageResizeException extends Exception {
        public ImageResizeException(String message, Throwable exception) {
            super(message, exception);
        }
    }

    public static class ImageEncodeException extends Exception {
        public ImageEncodeException(String message, Throwable exception) {
            super(message, exception);
        }
    }

    public static class ImageReadException extends Exception {
        public ImageReadException(String message, Throwable exception) {
            super(message, exception);
        }
    }

    public static byte[] resize(byte[] byteArrayImage, int width, int height) throws Exception {
        try {
            BufferedImage inputImage = byteArrayToBufferedImage(byteArrayImage);

            int baseWidth = inputImage.getWidth();
            int baseHeight = inputImage.getWidth();

            if(baseWidth > baseHeight) {
                int x = ( baseWidth - baseHeight ) / 2;
                inputImage = inputImage.getSubimage(x, 0, baseHeight+x, baseHeight);
            } else if (baseWidth > baseHeight) {
                int y = ( baseHeight - baseWidth ) / 2;
                inputImage = inputImage.getSubimage(0, y, baseWidth, baseWidth+y);
            }

            // creates output image
            BufferedImage outputImage = new BufferedImage(width, height, inputImage.getType());

            // scales the input image to the output image
            Graphics2D g2d = outputImage.createGraphics();
            g2d.drawImage(inputImage, 0, 0, width, height, null);
            g2d.dispose();

            return bufferedImageToByteArray(outputImage);
        } catch (Exception e) {
            throw new ImageResizeException("IMAGE_RESIZE_EXCEPTION", e);
        }
    }

    public static BufferedImage byteArrayToBufferedImage(byte[] byteArray) throws IOException {
        return ImageIO.read(new ByteArrayInputStream(byteArray));
    }

    public static byte[] bufferedImageToByteArray(BufferedImage bufferedImage) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, "jpg", baos );
        return baos.toByteArray();
    }

    public static String getFormatName(byte[] image) throws IOException {
        BufferedImage bufferedImage = byteArrayToBufferedImage(image);
        Iterator<ImageReader> readers = ImageIO.getImageReaders(bufferedImage);

        if (readers.hasNext()) {
            ImageReader read = readers.next();
            return read.getFormatName().toLowerCase();
        }

        return null;
    }

    public static void throwIfImageNotValid(byte[] image, String format) throws ImageFormatException {
        try {
            BufferedImage bufferedImage = byteArrayToBufferedImage(image);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ImageIO.write(bufferedImage, format, bos);
        } catch (Exception e) {
            throw new ImageFormatException("IMAGE_NOT_VALID_EXCEPTION", e);
        }
    }

    public static byte[] byteArrayEncodeToJPG(byte[] image) throws Exception {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            BufferedImage bi = byteArrayToBufferedImage(image);
            BufferedImage result = new BufferedImage(bi.getWidth(), bi.getHeight(), BufferedImage.TYPE_INT_RGB);
            result.createGraphics().drawImage(bi, 0, 0, Color.WHITE, null);
            ImageIO.write(result, "jpg", bos);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new ImageEncodeException("IMAGE_ENCODE_EXCEPTION", e);
        }
    }
}
