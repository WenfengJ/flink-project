package com.longyun.blink.java.kafka;

import java.io.Serializable;

/**
 * @author yuanxiaolong
 * @ClassName com.longyun.blink.java.kafka.Raw
 * @Description TODO
 * @Date 2019/2/26 13:13
 * @Version 1.0
 **/
public class Raw implements Serializable {

    private String key;

    private String raw;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    @Override
    public String toString() {
        return "Raw{" +
                "key='" + key + '\'' +
                ", raw='" + raw + '\'' +
                '}';
    }

    public static Raw of(String key, String raw){
        Raw ins = new Raw();
        ins.setKey(key);
        ins.setRaw(raw);
        return ins;
    }
}
