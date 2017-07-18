/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trellisldp.rosid.common;

import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.rdf.api.Dataset;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.slf4j.Logger;

/**
 * @author acoburn
 */
abstract class AbstractConsumerRunner implements Runnable {

    private static final Logger LOGGER = getLogger(AbstractConsumerRunner.class);
    private final long TIMEOUT = Long.parseLong(System.getProperty("kafka.poll.timeout.ms", "100"));

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final Consumer<String, Dataset> consumer;

    /**
     * A base consumer runner class, using the system-defined Kafka Consumer
     */
    protected AbstractConsumerRunner() {
        this(new KafkaConsumer<>(kafkaConsumerProps()));
        LOGGER.info("Initializing a kafka consumer with system-defined properties");
    }

    /**
     * A base consumer runner class, using the system-defined Kafka Consumer
     * @param properties the user-defined properties
     */
    protected AbstractConsumerRunner(final Properties properties) {
        this(new KafkaConsumer<>(properties));
        LOGGER.info("Initializing a kafka consumer with user-defined properties");
    }

    /**
     * A base consumer runner class
     * @param consumer the kafka consumer to use
     */
    protected AbstractConsumerRunner(final Consumer<String, Dataset> consumer) {
        requireNonNull(consumer, "the consumer may not be null!");
        //consumer.assign(topics);
        this.consumer = consumer;
    }

    /**
     * Manually assign the topic partitions
     * @param topics a collection of topic partitions
     */
    public void assign(final Collection<TopicPartition> topics) {
        consumer.assign(topics);
    }

    /**
     * Dynamically subscribe to a collection of topics
     * @param topics a collection of topics to subscribe to
     */
    public void subscribe(final Collection<String> topics) {
        consumer.subscribe(topics);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                handleRecords(consumer.poll(TIMEOUT));
            }
        } catch (final WakeupException ex) {
            if (!closed.get()) {
                throw ex;
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * Handle any retrieved records from the consumer
     * @param records the records
     */
    abstract protected void handleRecords(final ConsumerRecords<String, Dataset> records);

    /**
     * Shutdown the consumer
     */
    public void shutdown() {
        LOGGER.info("Shutting down kafka consumer");
        closed.set(true);
        consumer.wakeup();
    }

    private static Properties kafkaConsumerProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("kafka.bootstrap.servers"));
        props.put("group.id", System.getProperty("kafka.group.id", "trellis"));
        props.put("enable.auto.commit", System.getProperty("kafka.enable.auto.commit", "true"));
        props.put("auto.commit.interval.ms", System.getProperty("kafka.auto.commit.interval.ms", "1000"));
        props.put("session.timeout.ms", System.getProperty("kafka.session.timeout.ms", "30000"));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.trellisldp.rosid.common.DatasetSerializer");
        return props;
    }
}
