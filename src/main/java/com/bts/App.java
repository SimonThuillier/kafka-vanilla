package com.bts;

import com.bts.logging.AppLogger;
import io.github.cdimascio.dotenv.Dotenv;

// import clients.producer.KafkaProducer;
/**
 * kafka-vanilla
 *
 */
public class App
{
    private static final Dotenv dotenv = Dotenv.load();


    public static void main( String[] args )
    {
        // checking logger is ok
        AppLogger.getLogger().info("Starting Kafka-vanilla...");

        AppLogger.getLogger().info("Starting ApacheKafkaProducer...");

        VanillaKafkaProducer producer = new ApacheKafkaProducer();

        producer.start();
        AppLogger.getLogger().info("Running ApacheKafkaProducer.");
        producer.run();
        AppLogger.getLogger().info("Closing ApacheKafkaProducer.");
        producer.close();

        AppLogger.getLogger().info("Ending ApacheKafkaProducer.");
    }
}
