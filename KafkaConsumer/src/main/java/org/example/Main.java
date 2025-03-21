package org.example;

public class Main {
    public static void main(String[] args) {
        String topic = "test";
        ConsumerApp consumer = new ConsumerApp();
        consumer.receive(topic);
    }
}