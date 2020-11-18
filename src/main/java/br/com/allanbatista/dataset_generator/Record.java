package br.com.allanbatista.dataset_generator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.Objects;

public class Record implements Serializable {
    public String label;
    public String path;
    public Image image;
    public Long labelIndex;

    public static class Json {
        public String label;
        public String path;
        public Long label_index;

        public Json(Record record) {
            this.label = record.label;
            this.path = record.path;
            this.label_index = record.labelIndex;
        }
    }

    public Record(String label, String path) {
        this.label = label;
        this.path = path;
    }

    public Record(Record record){
        this.label = record.label;
        this.path = record.path;
        this.image = record.image;
        this.labelIndex = record.labelIndex;
    }

    public String toJson(){
        Gson gson = new GsonBuilder().create();
        return gson.toJson(new Json(this));
    }

    @Override
    public String toString() {
        return "Record{" +
                "label='" + label + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(label, record.label) &&
                Objects.equals(path, record.path) &&
                Objects.equals(image, record.image);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, path, image);
    }
}
