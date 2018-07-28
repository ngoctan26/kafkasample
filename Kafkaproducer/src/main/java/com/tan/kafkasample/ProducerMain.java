package com.tan.kafkasample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class ProducerMain {
    private static final String EXIT = "exit";

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = initializeProducer();
        while (true) {
            String topic = waitForInput("Input Topic: ");

            if (isExitSignal(topic)) {
                // Finished
                break;
            }
            String message = waitForInput("Input message: ");

            String partition = waitForInput("Input partition: ");

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, Integer.valueOf(partition), null, message);
            try {
                kafkaProducer.send(record, (recordMetadata, e) -> {
                    if (e != null) {
                        System.out.println("Send message failed: " + e.getMessage());
                    } else {
                        System.out.println("Send message successfully to topic: " + recordMetadata.topic() + " partition " + recordMetadata.partition() + " offset: " + recordMetadata.offset());
                    }
                });

                // Sleep for message log from call back showing.
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static KafkaProducer<String, String> initializeProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 60 * 1000); // 60s
        return new KafkaProducer<>(properties);
    }

    private static boolean isExitSignal(String inputString) {
        return EXIT.equals(inputString);
    }

    private static String waitForInput(String titleMessage) {
        String resultAsString;
        do {
            System.out.println(titleMessage);
            Scanner userInput = new Scanner(System.in);
            resultAsString = userInput.nextLine();
        } while (resultAsString == null || resultAsString.isEmpty());
        return resultAsString;
    }
}
