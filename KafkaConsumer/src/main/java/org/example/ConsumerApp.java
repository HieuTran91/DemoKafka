package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {
    public void receive(String topic)
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9094");
        properties.put("group.id", "my-consumer-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        int messageCount = 0;
        int total = 0;

        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records)
            {
                messageCount +=1;
                total += Integer.parseInt(record.value());
                System.out.printf("Received message: key = %s, value = %s, running average = %.2f%n",
                        record.key(), record.value(), (double) total / messageCount);
            }
        }
    }
}
