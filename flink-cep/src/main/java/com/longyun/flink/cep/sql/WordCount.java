package com.longyun.flink.cep.sql;

import java.io.Serializable;

/**
 * Java POJO 必须要有空的构造函数
 *
 * @author lynn
 * @ClassName com.longyun.flink.cep.sql.WordCount
 * @Description TODO
 * @Date 19-3-13 上午9:08
 * @Version 1.0
 **/
public class WordCount implements Serializable {
    private String word;
    private Integer frequency;

    public WordCount() {

    }

    public WordCount(String word, int frequency) {
        this(word, new Integer(frequency));
    }

    public WordCount(String word, Integer frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }
}
