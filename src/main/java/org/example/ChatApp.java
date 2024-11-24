package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class ChatApp {
    private final String sendTopic;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;

    public ChatApp(String sendTopic, String receiveTopic) {
        this.sendTopic = sendTopic;

        // Producer configuration
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(producerProps);

        // Consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("group.id", "chat-group");
        consumerProps.put("auto.offset.reset", "earliest");
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singletonList(receiveTopic));
    }

    public void startChat() {
        Thread sendThread = new Thread(this::sendMessages);
        Thread receiveThread = new Thread(this::receiveMessages);

        sendThread.start();
        receiveThread.start();

        try {
            sendThread.join();
            receiveThread.join();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());;
        }
    }

    private void sendMessages() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Type your messages (type 'exit' to quit):");
        try {
            while (true) {
                String message = scanner.nextLine();
                if ("exit".equalsIgnoreCase(message)) {
                    break;
                }
                ProducerRecord<String, String> record = new ProducerRecord<>(sendTopic, "user", message);
                producer.send(record);
            }
        } finally {
            producer.close();
        }
    }

    private void receiveMessages() {
        System.out.println("Listening for messages...");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    System.out.println("Received: " + record.value());
                });
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        // Topics for User 1 and User 2
        if(args.length<1){
            System.out.println("Using:java ChatApp <topic_name>");
            return;
        }
        String topic = args[0];
        String receiveTopic = args[1]; // e.g., "user2-to-user1"

        ChatApp chatApp = new ChatApp(topic, receiveTopic);
        chatApp.startChat();
    }
}
