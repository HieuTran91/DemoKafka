package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class ProducerApp {
    public void send(String topic) throws InterruptedException{

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        while(true)
        {
            int randomvalue = random.nextInt(100) + 1;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, Integer.toString(randomvalue));

            producer.send(record, (metadata, e)->{
                if(e==null)
                {
                    System.out.println("Message sent successfully to partition " + metadata.partition() + " at offset " + metadata.offset());
                }
                else {
                    e.printStackTrace();
                }
            });

//            int sleepTime = ThreadLocalRandom.current().nextInt(5000, 10000);
            Thread.sleep(5000);
        }
//        producer.close();
    }
}
