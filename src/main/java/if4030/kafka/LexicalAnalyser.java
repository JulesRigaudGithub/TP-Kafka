package if4030.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;
import java.util.Map;

final class Lemme {
    public final String word;
    public final String cat;

    public Lemme(String word, String category) {
        this.word = word;
        this.cat = category;
    }
}

public final class LexicalAnalyser {

    public static final String INPUT_TOPIC = "words-stream";
    public static final String OUTPUT_TOPIC = "tagged-words-stream";
    public static final String LEX_PATH = "lex/lexique.csv";

    private static Map<String, Lemme> lexicalMap = new HashMap<>();

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();

        try (final BufferedReader reader = new BufferedReader(new FileReader(LEX_PATH));) {
            String line = reader.readLine();
            while (line != null) {
                String[] splited = line.split(",");
                lexicalMap.put(splited[0], new Lemme(splited[1], splited[2]));

                line = reader.readLine();
            }
        }

        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "lexical-analyser");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    static void createLexicalStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        final KStream<String, String> transformedStream = source.map((key, value) -> {
            if (lexicalMap.containsKey(value)) {
                Lemme lemme = lexicalMap.get(value);
                return KeyValue.pair(lemme.word, lemme.cat);
            } else {
                return KeyValue.pair(value, "null");
            }
        });

        transformedStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        createLexicalStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
