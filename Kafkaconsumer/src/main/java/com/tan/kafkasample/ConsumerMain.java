package com.tan.kafkasample;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerMain {
    private static final int NUMBER_OF_CONSUMER = 3;
    private static final List<String> TOPICS = Arrays.asList("sample");

    public static void main(String[] args) throws InterruptedException {
        String scenarioType;
        do {
            scenarioType = waitForInput("Choose scenario: ");
        } while (scenarioType == null || scenarioType.isEmpty());
        String scenarioDefinition = "";
        switch (scenarioType) {
            case "1":
                scenarioDefinition = "Multiple consumer in different consumer group";
                break;
            case "2":
                scenarioDefinition = "Multiple consumer in same consumer group";
                break;
        }
        System.out.println("Start kafka sample with scenario: " + scenarioDefinition);

        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CONSUMER);
        List<SampleConsumer> sampleConsumers = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_CONSUMER; i++) {
            String consumerGroup = "group";
            if ("1".equals(scenarioType)) {
                consumerGroup += "_" + i;
            }
            SampleConsumer sampleConsumer = new SampleConsumer(consumerGroup, "consumer" + i, TOPICS, executorService);
            sampleConsumers.add(sampleConsumer);
            sampleConsumer.startConsume();
        }

        while (true) {
            String exit = waitForInput("");
            if ("exit".equals(exit)) {
                sampleConsumers.forEach((SampleConsumer::stopConsume));
                System.out.println("Stop scenario");
                break;
            }
        }

    }

    private static String waitForInput(String titleMessage) {
        System.out.println(titleMessage);
        Scanner userInput = new Scanner(System.in);
        return userInput.nextLine();
    }
}
