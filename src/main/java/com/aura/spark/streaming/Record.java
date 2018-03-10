package com.aura.spark.streaming;

public class Record
        implements java.io.Serializable {
    private String word;

    public Record(String word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
