package com.dh.kafka.producer;

import com.dh.kafka.config.Key;
import com.dh.kafka.config.Value;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        KafkaProducer<String,String> producer = null;
        try {
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<String, String>(properties);


            ProducerRecord msg = new ProducerRecord("test2topic", "m18");
            producer.send(msg, (RecordMetadata recordMetadata, Exception e) -> {
                if (e == null){
                    long offset = recordMetadata.offset();
                    int partition = recordMetadata.partition();
                    System.out.println("offset -> "+offset + " partition -> "+partition);
                    System.out.println("msg is sent");
                }
                else {
                    System.out.println("msg sent is failed");
                }

            } );

            ProducerRecord msg2 = new ProducerRecord("test2topic", "id1", "m19");
            producer.send(msg2, (RecordMetadata recordMetadata, Exception e) -> {
                if (e == null){
                    long offset = recordMetadata.offset();
                    int partition = recordMetadata.partition();
                    System.out.println("offset -> "+offset + " partition -> "+partition);
                    System.out.println("msg2 is sent");
                }
                else {
                    System.out.println("msg sent is failed");
                }

            } );

            producer.flush();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
