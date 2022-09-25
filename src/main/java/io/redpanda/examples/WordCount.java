/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.redpanda.examples;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class WordCount {

    //    final static String inputTopic = "soso";
//    final static String inputTopic = "flink.public.users"; // Kalo pake debezium connect nightly
    final static String inputTopic = "testadit.public.users"; // Kalo pake debezium 1.9
    final static String outputTopic = "salsa";
    //    final static String outputTopic = "salsa";
    final static String jobTitle = "WordCount";

    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    static class SyncLog {
        public SyncLog(String uuid, String data) {
            this.uuid = uuid;
            this.data = data;
        }

        final String uuid;
        final String data;
    }

    final static OutputTag<SyncLog> outputTag = new OutputTag<SyncLog>("side-output") {
    };

    public static void main(String[] args) throws Exception {
//        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        LOG.info("Started " + jobTitle);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("kafka:9092").setTopics(inputTopic).setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();

        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder().setValueSerializationSchema(new SimpleStringSchema()).setTopic(outputTopic).build();

        KafkaSink<String> sink = KafkaSink.<String>builder().setBootstrapServers("kafka:9092").setRecordSerializer(serializer).build();

        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

//        text.sinkTo(sink);

        SingleOutputStreamOperator<SyncLog> mainDataStream = text.process(new firstProcess());
        DataStream<SyncLog> sideOutputStream = mainDataStream.getSideOutput(outputTag);

        DataStreamSink<SyncLog> dd = sideOutputStream.addSink(JdbcSink.sink(
                "insert into dadang (uuid, data) values (?, ?)",
                (statement, syncLog) -> {
                    statement.setString(1, syncLog.uuid);
                    statement.setString(2, syncLog.data);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://postgresql_db:5432/sync_test_log")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("postgres")
                        .withPassword("secret")
                        .build()
        ));


//        DataStream<SyncLog> sideOutputStream = mainDataStream.getSideOutput(outputTag);
//        env.fromElements(
//                new SyncLog(UUID.randomUUID().toString(), "Stream Processing with Apache Flink")
//        ).addSink(
//                JdbcSink.sink(
//                        "insert into dadang (uuid, data) values (?, ?)",
//                        (statement, syncLog) -> {
//                            statement.setString(1, syncLog.uuid);
//                            statement.setString(2, syncLog.data);
//                        },
//                        JdbcExecutionOptions.builder()
//                                .withBatchSize(1000)
//                                .withBatchIntervalMs(200)
//                                .withMaxRetries(5)
//                                .build(),
//                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                                .withUrl("jdbc:postgresql://postgresql_db:5432/sync_test_log")
//                                .withDriverName("org.postgresql.Driver")
//                                .withUsername("postgres")
//                                .withPassword("secret")
//                                .build()
//                ));


        //        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("kafka:9092").setTopics(inputTopic).setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
//
//        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySourceName");
//        stream.print();

        env.execute(jobTitle);
    }

    public static final class firstProcess extends ProcessFunction<String, SyncLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<SyncLog> out) throws Exception {
            ObjectMapper mapper = new ObjectMapper();

            JsonNode root = mapper.readTree(value);
            JsonNode payload = root.get("payload");
            LOG.info("Pretty print");
            LOG.info(payload.get("after").toPrettyString());
            // emit data to regular output
//            out.collect(String.valueOf(value));

            // emit data to side output
            ctx.output(outputTag, new SyncLog(UUID.randomUUID().toString(), "Hello World"));
//                System.out.println("Testing " + value);
//                LOG.info("Haha Masuk");
        }
    }
}
