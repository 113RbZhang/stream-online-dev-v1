package com.rb.black_list.codes;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.black_list.utils.SensitiveWordsUtils;
import com.rb.fuction.DimAsync;
import com.rb.utils.CheckPointUtils;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.rb.black_list.codes.Processing_Data
 * @Author runbo.zhang
 * @Date 2025/5/7 16:24
 * @description:
 */
public class BlackListProcessingData {
    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.newSetCk(env, "BlackListProcessingData");
        //todo
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaReadSetWater(env, "log_topic_flink_online_v1");

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaRead.map(o -> JSON.parseObject(o));

        SingleOutputStreamOperator<JSONObject>  after = jsonDs.filter(new RichFilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {

                return false;
            }
        });

        SingleOutputStreamOperator<JSONObject> OrderInfoDs = jsonDs
                .filter(o -> "order_info".equals(o.getJSONObject("source").getString("table")) && o.containsKey("after"))
                .uid("OrderInfoDs").name("OrderInfoDs");


        //{"op":"c","after":{"create_time":1744123351000,"user_id":70,"appraise":"1201","comment_txt":"评论内容：57291398556419952713137891165429431798643343275826","nick_name":"伊亚","sku_id":21,"id":29,"spu_id":6,"order_id":462},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000002","connector":"mysql","pos":8928063,"name":"mysql_binlog_source","row":0,"ts_ms":1744092486000,"snapshot":"false","db":"online_flink_retail","table":"comment_info"},"ts_ms":1745225178936}
        SingleOutputStreamOperator<JSONObject> commentDs = jsonDs
                .filter(o -> "comment_info".equals(o.getJSONObject("source").getString("table")))
                .uid("commentDs").name("commentDs");
//        commentDs.print();
//        commentDs.filter(new RichFilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                System.out.println(value.containsKey("after")+"="+value.getString("after"));
//                return value.containsKey("after");
//            }
//        });



        DataStream<JSONObject> keyedStream = commentDs.filter(o->o.getString("after")!=null ).keyBy(o -> o.getJSONObject("after").getString("appraise"));

//        keyedStream.print();

        //{"op":"c","after":{"create_time":1744204944000,"user_id":118,"appraise":"1201","comment_txt":"评论内容：27886673813985174491147526819254467889267492463873","nick_name":"巧美","sku_id":33,"id":49,"spu_id":11,"order_id":632},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000002","connector":"mysql","pos":11399512,"name":"mysql_binlog_source","row":0,"ts_ms":1744092543000,"snapshot":"false","db":"online_flink_retail","table":"comment_info"},"ts_ms":1745225182409}
        SingleOutputStreamOperator<JSONObject> asyncDs = AsyncDataStream.unorderedWait(keyedStream, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                obj.put("dic_name", dimJsonObj.getString("dic_name"));
            }
            @Override
            public String getTableName() {
                return "dim_base_dic";
            }
            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getJSONObject("after").getString("appraise");
            }
        }, 100, TimeUnit.SECONDS).uid("asyncDs").name("asyncDs");
    //asyncDs.print();


        env.disableOperatorChaining();
        env.execute();

    }


}
