package br.com.allanbatista.dataset_generator;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class Image implements Serializable {
    private byte[] bytes;
    private String format;

    public Image(byte[] bytes, String format) {
        this.bytes = bytes;
        this.format = format;
    }

    public Image encodeToJPEG() throws Exception {
        return new Image(ImageHelpers.byteArrayEncodeToJPG(bytes), "jpeg");
    }

    public byte[] getBytes() {
        return bytes;
    }

    public String getFormat() {
        return format;
    }

    public String getMimeType() {
        return "image/"+format;
    }

    public Image resize(int width, int height) throws Exception {
        return new Image(ImageHelpers.resize(bytes, width, height), format);
    }

    public void throwIfImageNotValid() throws Exception {
        ImageHelpers.throwIfImageNotValid(bytes, format);
    }

    @Override
    public String toString() {
        return "Image{" +
                ", format='" + format + '\'' +
                ", mimeType='" + getMimeType() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Image image = (Image) o;
        return Arrays.equals(bytes, image.bytes) &&
                Objects.equals(format, image.format);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(format);
        result = 31 * result + Arrays.hashCode(bytes);
        return result;
    }
}
