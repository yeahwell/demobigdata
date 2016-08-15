package com.webmovie.bigdata.akka.messages;

/**
 * Created by yeahwell on 16/8/14.
 */
public class WordCount {

    private String word;

    private Integer count;

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public Integer getCount() {
        return count;
    }

    public String getWord() {
        return word;
    }
}
