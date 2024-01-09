package ru.didcvee.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static void main(String[] args) {
        // зависимости кафки
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // создание продюсера

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // создание продсер-рекорд

        ProducerRecord<String, String> record =
                new ProducerRecord<>("my-topic", "hello world");

        // отправка данных - асинхронно, поэтому надо зафлашить

        producer.send(record);

        // флаш и клоуз продюсера
        producer.flush();
        producer.close();

    }
}
