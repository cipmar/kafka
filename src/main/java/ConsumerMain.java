import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.singletonList;

public class ConsumerMain {
    public static void main(String[] args) throws InterruptedException {
        final int NO_OF_CONSUMERS = 3;
        ExecutorService executorService = Executors.newFixedThreadPool(NO_OF_CONSUMERS);

        List<Consumer<String, String>> consumers = IntStream.range(0, NO_OF_CONSUMERS).mapToObj(i -> newConsumer()).collect(Collectors.toList());
        consumers.forEach(consumer -> executorService.execute(new ConsumerTask(consumer)));

        executorService.shutdown(); // don't accept more tasks
        executorService.awaitTermination(1, TimeUnit.MINUTES); // wait

        consumers.forEach(Consumer::wakeup); // gracefully stops consumer
    }

    private static Consumer<String, String> newConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "group1");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(singletonList("mytopic1"));

        return consumer;
    }

    private static class ConsumerTask implements Runnable {
        private Consumer<String, String> consumer;

        public ConsumerTask(Consumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, MILLIS));

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(String.format("thread: %d, partition: %s, key: %s, value: %s",
                                Thread.currentThread().getId(), record.partition(), record.key(), record.value()));
                    }
                }
            } catch (WakeupException e) {
                System.out.println("Closing consumer...");
            } finally {
                consumer.close();
            }
        }
    }
}
