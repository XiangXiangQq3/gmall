package com.xstudio.gmall.flume.interceptor;

        import org.apache.commons.lang.StringUtils;
        import org.apache.commons.lang.math.NumberUtils;

/**
 * @author Xiangxiang
 * @date 2020/3/10 20:26
 * this is a log utils
 * ！
 */
public class LogUtils {
    public static boolean validateStart(String log) {
        //判断是否为空
        if (StringUtils.isBlank(log)) {
            return false;
        }
        //取反 判断数据是否为启动日志
        if (!log.startsWith("{") || !log.endsWith("}")) {
            return false;
        }
        return true;
    }

    public static boolean validateEvent(String log) {
        if (StringUtils.isBlank(log)) {
            return false;
        }
        String[] splits = log.split("\\|");  //这里传的是正则表达式
        String timeStamp = splits[0];  //获取时间戳
        //获取后面json数组
        String json = splits[1];
        //校验时间戳长度
        if (timeStamp.length() != 13) {
            return false;
        }
        //判读时间戳为数字
        if (!NumberUtils.isDigits(timeStamp)) {
            return false;
        }
        //判断json数组
        if (!json.startsWith("{") || !json.endsWith("}")) {
            return false;
        }
        return true;
    }
}
