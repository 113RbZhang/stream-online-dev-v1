package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.CheckPointUtils;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Package com.rb.dws.DwsNewPagViewWindow
 * @Author runbo.zhang
 * @Date 2025/4/28 17:43
 * @description:
 */
public class DwsNewPagViewWindow {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.setCk(env);
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "log_topic_flink_online_v2_log_page");

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaRead.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {

                JSONObject s = null;
                try {
                    s = JSON.parseObject(value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return s;
            }
        });
        SingleOutputStreamOperator<JSONObject> mapData = jsonDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("", String.class);
                lastVisitState = getRuntimeContext().getState(valueStateDescriptor);

            }
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                String lastDate = lastVisitState.value();
                Long ts = jsonObject.getLong("ts");
                String date = DateFormatUtil.tsToDate(ts);
                Long uvCt = 0L;
                if (StringUtils.isEmpty(lastDate) || !lastDate.equals(date)) {
                    uvCt = 1L;
                    lastVisitState.update(date);
                }
                String lastPageId = page.getString("last_page_id");
                Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
                jsonObject.put("uvCt", uvCt);
                jsonObject.put("svCt", svCt);
                return jsonObject;
            }
        });
        //设置水位线
        SingleOutputStreamOperator<JSONObject> watermarksDs = mapData.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));
        //根据ar地区  ch渠道 vc版本 is_new 新用户分组
        KeyedStream<JSONObject, Tuple4<String, String, String, String>> keyedStream = watermarksDs.keyBy(new KeySelector<JSONObject, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(JSONObject value) throws Exception {
                JSONObject common = value.getJSONObject("common");
                String ar = common.getString("ar");
                String ch = common.getString("ch");
                String isNew = common.getString("is_new");
                String vc = common.getString("vc");

                return Tuple4.of(ar, ch, vc, isNew);
            }
        });
        keyedStream.print();


        env.execute();
    }
}
