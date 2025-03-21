package org.example;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String topic = "test";

        ProducerApp producer = new ProducerApp();
        producer.send(topic);
    }
}