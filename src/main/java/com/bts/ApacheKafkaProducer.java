package com.bts;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Date;
import java.util.Properties;

public class ApacheKafkaProducer implements VanillaKafkaProducer {

    private static final Dotenv dotenv = Dotenv.load();
    private static final Properties props = new Properties();
    private Producer<String, String> producer;

    public ApacheKafkaProducer(){
        props.put("bootstrap.servers", dotenv.get("KAFKA_BOOTSTRAP_SERVER"));

        props.put("linger.ms", 1);
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", Integer.parseInt(dotenv.get("EVENT_PRODUCTION_BATCH_SIZE")));

        props.put("max.block.ms", 1000*Integer.parseInt(dotenv.get("EVENT_PRODUCTION_TIMEOUT")));

        props.put("transactional.id", "apacheKafka-" + (new Date()).getTime());
        props.put("enable.idempotence", true);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void start(){
        producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }

    public void run(){

        try {
            producer.beginTransaction();
            int currentBatchIndex = 0;
            for (int i = 0; i < Integer.parseInt(dotenv.get("EVENT_PRODUCTION_COUNT")); i++){
                producer.send(new ProducerRecord<>(dotenv.get("TOPIC"), Integer.toString(i), Integer.toString(i)));
                currentBatchIndex++;
                if (currentBatchIndex >= Integer.parseInt(dotenv.get("EVENT_PRODUCTION_BATCH_SIZE"))){
                    producer.commitTransaction();
                    producer.beginTransaction();
                    currentBatchIndex = 0;
                }
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            this.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
    }

    public void close(){
        try{
            producer.close();
        }
        finally {
            producer = null;
        }
    }
}