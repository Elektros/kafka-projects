package gmbh.conteco;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionalProducer {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger("TransactionalProducer");

        logger.info("Entered main");
        Properties properties = new Properties();

        try (InputStream stream = TransactionalProducer.class.getResourceAsStream("/application.properties")) {
            properties.load(stream);
            logger.info("Loaded Properties");
        } catch (IOException e) {
            logger.info("Load Properties failed");
            throw new RuntimeException(e);
        }

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            Scanner scanner = new Scanner(System.in);

            logger.info("init transactions");

            try {
                producer.initTransactions();
            } catch (RuntimeException e) {
                logger.warning(e.getMessage());
                throw new RuntimeException(e.getMessage());
            }

            logger.info("begin transactions");
            producer.beginTransaction();

            int messages_in_transaction = 0;
            long amount_of_message = 0;
            String topic = properties.getProperty("topic");

            while (true) {
                System.out.println("Press enter to commit messages or type in a new message:");
                String text = scanner.nextLine();
                logger.info("Last input: " + text);
                logger.info("Messages in transaction: " + messages_in_transaction);
                logger.info("Amount of message: " + amount_of_message);

                if (text.equals("")) {
                    if (messages_in_transaction == 0) {
                        logger.warning("abort producer...");
                        producer.abortTransaction();
                        return;
                    }

                    producer.commitTransaction();
                    logger.info("transaction commited");
                    producer.beginTransaction();
                    messages_in_transaction = 0;
                    continue;
                }

                messages_in_transaction++;

                producer.send(new ProducerRecord<>(topic, String.format("Messages in transaction: %d, Number of message: %d", messages_in_transaction, amount_of_message), text));
                logger.info("message sent");
                amount_of_message++;
            }
        }
    }
}