package com.xstudio.gmall.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Xiangxiang
 * @date 2020/3/16 14:49
 * 对UDF获取的et数组进行处理，炸开et 得到其json数组的里面每一个事件
 */
public class LogUDTF extends GenericUDTF {


    /**
     * A custom UDTF can be created by extending the GenericUDTF abstract class and
     * then implementing the initialize, process, and possibly close methods.
     * The initialize method is called by Hive to notify the UDTF the argument types to expect.
     * The UDTF must then return an object inspector corresponding to the row objects
     * that the UDTF will generate.
     * Once initialize() has been called,
     * Hive will give rows to the UDTF using the process() method.
     * While in process(), the UDTF can produce and forward rows to other operators
     * by calling forward().
     * Lastly, Hive will call the close() method
     * when all the rows have passed to the UDTF.
     * 1.获取参数信息进行校验；2返回输出类型
     * @param argOIs
     * @return 返回传参的修改过后的输出类型
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //返回一个集合，集合里面是元素的个数
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        if (allStructFieldRefs.size() != 1){
            throw new UDFArgumentException("参数的个数只能是1");
        }

        //获取参数,获取其检查器的名称
        String typeName = allStructFieldRefs.get(0).getFieldObjectInspector().getTypeName();
        if (!"string".equals(typeName)){
            throw new UDFArgumentException("参数只能是string类型");
        }



        //这是hive干的事情，样板已经写好了，将上述过滤好的参数类型传入下列 hive框架会自己获取参数
        //炸开可以多列，将想炸开的列名添加到集合中，一个列有一个对象检查器
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("event_name");
        fieldNames.add("event_json");
        //申明检查器的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //封装成StructObjectInspector返回
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //传入的只有一个参数,传入的参数是et json数组
        String events = args[0].toString(); //返回events

        //将字符串类型的json数组作为参数，解析json 返回一个json的数组
        JSONArray jsonArray = new JSONArray(events);

        for (int i = 0; i < jsonArray.length(); i++) {

            //声明两列
            String[] result = new String[2];
            //获取json数组更内层的json
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            //声明第一列 事件名称是
                  result[0] = jsonObject.getString("en");
            //声明第二列 获取该事件的json ，如果想要获取该json更内层的字段 ，调用之前写的udf函数
            result[1] = jsonArray.getString(i);

            //将两列的集合交给forward
            forward(result);

        }
    }

    public void close() throws HiveException {

    }
}
