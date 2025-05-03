package com.rb;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.rb.Test
 * @Author runbo.zhang
 * @Date 2025/5/3 15:43
 * @description:
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> kafkaRead = SourceSinkUtils.cdcRead(env, "online_flink_retail", "*");
        kafkaRead.print();
        env.disableOperatorChaining();
        env.execute();

    }
}
