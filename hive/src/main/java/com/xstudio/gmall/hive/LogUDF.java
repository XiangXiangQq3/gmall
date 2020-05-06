package com.xstudio.gmall.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;


/**
 * @author Xiangxiang
 * @date 2020/3/16 14:47
 *
 * 写UDF函数 获取Json里面的字段
 */
public class LogUDF extends UDF {

    //输入line  输出key 对应的value   key是在参数中输入的字符
    public String evaluate(String line,String key){

        String[] split = line.split("\\|");
        if (split.length != 2){
            return "";
        }
        String serverTime = split[0];
        String json = split[1];

        //Json解析
        JSONObject base = new JSONObject(json);

        if ("st".equals(key)){
            return serverTime;
        }else if ("et".equals(key)){ //可以获取事件Json数组
            if (base.has("et")) {//获取et后面的json数组
                return base.getString("et");
            }
        }else {
            JSONObject cm = base.getJSONObject("cm");//拿到cm的子json
            if (cm.has(key)) {
                return cm.getString(key);//传什么获取什么json字段
            }
        }

        return "";
    }

}
