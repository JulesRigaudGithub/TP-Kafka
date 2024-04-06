package if4030.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;


public final class WordCount {

    public static final String INPUT_TOPIC = "tagged-words-stream";
    public static String CATEGORY = "";

    private static Properties getConsumerConfig(final String[] args) throws IOException {
        final Properties props = new Properties();

        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "streams-wordcount");
        props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static KafkaConsumer<String, String> createWordCountConsumer(final Properties props) {
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("tagged-words-stream", "command-topic"));
        return consumer;
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getConsumerConfig(args);
        final KafkaConsumer<String, String> consumer = createWordCountConsumer(props);

        final Map<String, Map<String, Long>> counters = new HashMap<>();
        
        boolean running = true;
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String topic = record.topic();
                if ("tagged-words-stream".equals(topic)) {
                   handleLemme(counters, record);
                } else if ("command-topic".equals(topic)) {
                    running = handleCommand(counters, record);
                }
            }
        }
    }

    private static void handleLemme(Map<String, Map<String, Long>> counters, ConsumerRecord<String, String> record) {
        String word = record.key();
        String category = record.value();

        if (filterByCategory(category)) {
            category = category.substring(0, 3);
            if (!counters.containsKey(category)) {
                counters.put(category, new HashMap<>());
            }

            Map<String, Long> counter = counters.get(category);
            counter.put(word, counter.getOrDefault(word, 0L) + 1);
        }
    }

    private static boolean handleCommand(Map<String, Map<String, Long>> counters, ConsumerRecord<String, String> record) {
        String[] splited_command = record.value().split(" ");

        if (splited_command.length < 1) {
            return true;
        }

        System.out.println("[*] Command received: " + splited_command[0]);

        if ("END".equals(splited_command[0])) {
            printResult(counters, 20);
            return false;
        }

        return true;
    }

    private static boolean filterByCategory(String category) {
        String sub = category.substring(0, 3);
        return !sub.equals("ART") && !sub.equals("PRO") && !sub.equals("null");
    }

    private static void printResult(Map<String, Map<String, Long>> counters, int limit) {
        if (counters.isEmpty() || 
            (!CATEGORY.equals("") && !counters.containsKey(CATEGORY))) {
            System.out.println("No data to display");
        }

        for (Map.Entry<String, Map<String, Long>> category : counters.entrySet()) {
            if (!CATEGORY.equals("") && !category.getKey().equals(CATEGORY)) {
                continue;
            }

            System.out.println("---------------------------------------------");
            System.out.println("CATEGORY: " + category.getKey());
            System.out.println("---------------------------------------------");

            if (category.getValue().isEmpty()) {
                System.out.println("No data to display");
            }

            printTopOccurrences(category.getValue(), limit);
        }
    }

    private static void printTopOccurrences(Map<String, Long> counter, int limit) {
        counter.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .limit(limit)
            .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
    }
}