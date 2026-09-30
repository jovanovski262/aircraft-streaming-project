package com.mk.ukim.finki;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class AircraftProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "aircraft-raw";

    private static final long POLLING_INTERVAL_MS = 10_000;

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        OpenSkyClient openSkyClient = new OpenSkyClient();
        Gson gson = new Gson();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println("\nStopping aircraft producer...");
                    producer.flush();
                    producer.close();
                })
        );

        while (true) {

            try {

                List<AircraftEvent> aircraftEvents =
                        openSkyClient.fetchAircraft();

                System.out.println(
                        "Aircraft received from OpenSky: "
                                + aircraftEvents.size()
                );

                for (AircraftEvent aircraftEvent : aircraftEvents) {

                    String json = gson.toJson(aircraftEvent);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(
                                    TOPIC,
                                    aircraftEvent.getIcao24(),
                                    json
                            );

                    producer.send(record);
                }

                producer.flush();

                System.out.println(
                        "Sent "
                                + aircraftEvents.size()
                                + " aircraft events to Kafka."
                );

                System.out.println("Waiting 10 seconds...\n");

                Thread.sleep(POLLING_INTERVAL_MS);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;

            } catch (Exception e) {

                System.err.println(
                        "Error fetching/sending aircraft data: "
                                + e.getMessage()
                );

                try {
                    Thread.sleep(POLLING_INTERVAL_MS);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}