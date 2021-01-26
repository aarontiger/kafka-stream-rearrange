package com.bingoyes.kafka.stream.rearrange;

import com.bingoyes.kafka.rearrange.manual.MessageRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SingleStreamRearrangeThread extends Thread{

    private String inputTopic;
    private String outputTopic;
    private CountDownLatch latch1;
    private CountDownLatch latch2;

    private ThreadStatus threadStatus;

    public SingleStreamRearrangeThread(CountDownLatch latch1,CountDownLatch latch2){
        this.latch1 = latch1;
        this.latch2 = latch2;


    }

    //得到最新一批数据
    public List<MessageRecord> getOrderedBatchData(){
        return null;
    }

    //启动处理下一个窗口的数据
    public void nextWindow(){

    }

    //输出到
    public void outputStream(List<MessageRecord> dataList){
        threadStatus.setStartTime(null);
        threadStatus.setEndTime(null);
        threadStatus.setStatus(1);
        latch1.countDown();
    }

    @Override
    public void run(){
        try {
            initKafkaStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initKafkaStream() throws IOException {
        final Properties props = new Properties();


        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.60.219:9092");
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Topology builder = new Topology();

        builder.addSource("Source", "streams-plaintext-input");

        builder.addProcessor("Process", new MyProcessorSupplier(), "Source");
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("Counts"),
                Serdes.String(),
                Serdes.Integer()),
                "Process");

        builder.addSink("Sink", "streams-wordcount-processor-output", "Process");

        final KafkaStreams streams = new KafkaStreams(builder, props);
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


    static class MyProcessorSupplier implements ProcessorSupplier<String, String, String, String> {

        @Override
        public Processor<String, String, String, String> get() {
            return new Processor<String, String, String, String>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext<String, String> context) {
                    this.context = context;
                    this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                        try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
                            System.out.println("----------- " + timestamp + " ----------- ");

                            while (iter.hasNext()) {
                                final KeyValue<String, Integer> entry = iter.next();

                                System.out.println("[" + entry.key + ", " + entry.value + "]");

                                context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                            }
                        }
                    });
                    kvStore = context.getStateStore("Counts");
                }

                @Override
                public void process(final Record<String, String> record) {
                    final String[] words = record.value().toLowerCase(Locale.getDefault()).split(" ");

                    for (final String word : words) {
                        final Integer oldValue = kvStore.get(word);

                        if (oldValue == null) {
                            kvStore.put(word, 1);
                        } else {
                            kvStore.put(word, oldValue + 1);
                        }
                    }
                }
            };
        }
    }

}
