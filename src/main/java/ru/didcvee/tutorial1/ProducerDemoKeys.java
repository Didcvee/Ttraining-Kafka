package ru.didcvee.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String TOPIC_NAME = "aloha";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // зависимости кафки
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // создание продюсера

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // создание продсер-рекорд
        for (int i = 0; i<9; i++) {
            String value = "hello world "+ i;
            String key = "id_"+i;

            log.info("Key: "+ i);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME,key, value);

            // отправка данных - асинхронно, поэтому надо зафлашить

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // выполняется всегда когда отправка рекорда успешная или выбрасывается исключение
                    if (e == null) {
                        log.info("Отправка новых данных. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing");
                    }
                }
            }).get(); // блокируем send()
        }

        // флаш и клоуз продюсера
        producer.flush();
        producer.close();

    }
}
