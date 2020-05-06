package com.xstudio.gmall.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

/**
 * @author Xiangxiang
 * @date 2020/3/10 20:22
 * this is etl ~ ~ ! ! !
 */
public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {
    }
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("utf8"));
        if (log.contains("\"en\":\"start\"")) { //启动日志
            if (LogUtils.validateStart(log)) {
                return event;
            }
        } else { //事件日志
            if (LogUtils.validateEvent(log)) {
                return event;
            }
        }
        return null;
    }
    @Override
    public List<Event> intercept(List<Event> events) {
        //将不符合要求的event踢出去
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()) {
            Event next = iterator.next();
            if (intercept(next) == null) {//调用上面itercept方法
                iterator.remove();
            }
        }
        return events;
    }
    @Override
    public void close() {
    }
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }
        @Override
        public void configure(Context context) {
        }
    }
}
