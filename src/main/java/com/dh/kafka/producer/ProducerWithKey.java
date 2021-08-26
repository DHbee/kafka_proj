package com.dh.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerWithKey {

    public static void main(String[] args) {

        KafkaProducer<String,String> producer = null;
        try {
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<String, String>(properties);


            for (int i=10; i< 30; i++) {
                ProducerRecord msg2 = new ProducerRecord("test2topic", "id_"+0, "msg_"+i);
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

                } ).get();
            }

            producer.flush();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
