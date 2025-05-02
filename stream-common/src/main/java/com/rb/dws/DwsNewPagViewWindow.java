package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.CheckPointUtils;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        jsonDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            ValueState<String> lastVisitState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("", String.class);
                lastVisitState=getRuntimeContext().getState(valueStateDescriptor);

            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                return null;
            }
        });

        env.execute();
    }
}
