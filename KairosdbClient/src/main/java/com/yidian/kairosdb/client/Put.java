package com.yidian.kairosdb.client;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author weijian
 * Date : 2015-12-15 12:16
 */

public final class Put {

    private final String metric;

    private final long ts;

    private final Number value;

    private final Map<String, String> tags;

    private Put(String metric, long ts, Number value, Map<String, String> tags) {
        this.metric = checkNotNull(metric, "metric");
        this.value = checkNotNull(value, "value");
        this.tags = checkNotNull(tags, "tags");
        this.ts = ts;
    }


    private static <T> T checkNotNull(T obj, String arg){
        if (obj == null){
            throw new NullPointerException(arg + "is null;");
        }
        return obj;
    }

    public static Put of(String metric, long ts, Number value, Map<String, String> tags){
        return new Put(metric, ts, value, tags);
    }

    public static Put of(String metric, long ts, Number value){
        return of(metric, ts, value, Collections.emptyMap());
    }

    public static Put of(String metric, long ts, Number value,
                         String tagKey, String tagVal){
        return of(metric, ts, value, new HashMap<String, String>(){{put(tagKey, tagVal);}});
    }

    public static Put of(String metric, long ts, Number value,
                         String tagKey1, String tagVal1,
                         String tagKey2, String tagVal2){
        return of(metric, ts, value,
                new HashMap<String, String>(){{
                    put(tagKey1, tagVal1);
                    put(tagKey2, tagVal2);
                }}
        );
    }

    public static Put of(String metric, long ts, Number value,
                         String tagKey1, String tagVal1,
                         String tagKey2, String tagVal2,
                         String tagKey3, String tagVal3){
        return of(metric, ts, value,
                new HashMap<String, String>(){{
                    put(tagKey1, tagVal1);
                    put(tagKey2, tagVal2);
                    put(tagKey3, tagVal3);
                }}
        );
    }


    //  put <metric name> <time stamp> <value> <tag> <tag>... \n
    public StringBuilder writeTo(StringBuilder sb){
        sb.append("put ").append(metric).append(' ')
                .append(ts).append(' ').append(value);
        if (!tags.isEmpty()){
            tags.entrySet().forEach(
                    e -> sb.append(' ').append(e.getKey()).append('=').append(e.getValue())
            );
        }
        sb.append('\n');
        return sb;
    }

    public String cmdString(){
        return writeTo(new StringBuilder()).toString();
    }
}
