package com.cw.flink.chapter05datastream.source;

import java.sql.Timestamp;

/**
 * Flink 对 POJO 类型的要求如下：
 * 类是公共的（public）和独立的（standalone，也就是说没有非静态的内部类）
 * 类有一个公共的无参构造方法
 * 类中的所有字段是 public 且非 final 的；
 * 或者有一个公共的 getter 和 setter 方法，这些方法需要符合 Java bean 的命名规范
 * 所有属性的类型都是可以序列化的
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
