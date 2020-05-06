package com.xstudio.gmall.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author Xiangxiang
 * @date 2020/3/10 20:03
 * 将日志 分类
 * 将start归类，event分类写入header里 ，往event里面打标签
 */
public class TypeInterceptor implements Interceptor {
    public void initialize() {
    }

    public Event intercept(Event event) {
        //获取event的字节数组
        byte[] body = event.getBody();
        //对字节数组进行编码，获取一条log日志
        String log = new String(body, Charset.forName("utf8"));
        //对日志进行判断
        //往event里面打标签
        if (log.contains("\"en\":\"start\"")) { //如果包含start则
            event.getHeaders().put("topic", "topic_start"); //向event的header里添加属性 k=topic，v=channel
        } else {
            event.getHeaders().put("topic", "topic_event");
        }
        return event;
    }
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }
    public void close() {
    }
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }
        @Override
        public void configure(Context context) {
            //这里配置文件加载的flume配置的文件
            //a1.sources.r1.interceptors.i1.conf = .....
        }
    }
}
