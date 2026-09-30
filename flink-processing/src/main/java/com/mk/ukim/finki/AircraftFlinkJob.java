package com.mk.ukim.finki;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AircraftFlinkJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka:29092")
                        .setTopics("aircraft-raw")
                        .setGroupId("aircraft-flink-consumer")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        DataStream<String> aircraftStream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Aircraft Kafka Source"
                );

        DataStream<AircraftEvent> aircraftEvents =
                aircraftStream
                        .map(new AircraftJsonDeserializer())
                        .filter(event ->
                                event.getLatitude() != null &&
                                        event.getLongitude() != null
                        );

        DataStream<ProcessedAircraftEvent> processedAircraftEvents =
                aircraftEvents
                        .map(new AircraftTransformer());

        processedAircraftEvents.print();

        processedAircraftEvents
                .sinkTo(new ClickHouseSink())
                .name("ClickHouse Sink");

        processedAircraftEvents
                .sinkTo(new TimescaleDBSink())
                .name("TimescaleDB Sink");

        env.execute("Aircraft Streaming Job");
    }
}