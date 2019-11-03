import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class ProducerMain {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        Scanner scanner = new Scanner(System.in);
        Random random = new Random(System.currentTimeMillis());
        String message = "";

        while (!message.equals("end")) {
            message = scanner.next();
            int key = random.nextInt() & Integer.MAX_VALUE;
            int partition = key % 3;

            if (!message.equals("end")) {
                System.out.println("sending message: '" + message + "' with key " + key + " to partition " + partition);
                ProducerRecord<String, String> record = new ProducerRecord<>("mytopic1", partition, String.valueOf(key), message);
                kafkaProducer.send(record);
                System.out.println("message sent");
            }
        }

        kafkaProducer.close();
    }
}
