/*
 * Copyright Amherst College
 *
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
package edu.amherst.acdc.trellis.rosid;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * @author acoburn
 */
public class AbstractConsumerRunnerTest {

    public static class MyConsumerRunner extends AbstractConsumerRunner {

        private CompletableFuture<ConsumerRecords<String, Message>> future;

        public MyConsumerRunner(final Collection<TopicPartition> topics, final Consumer<String, Message> consumer,
                final CompletableFuture<ConsumerRecords<String, Message>> future) {
            super(topics, consumer);
            this.future = future;
        }

        @Override
        protected void handleRecords(final ConsumerRecords<String, Message> records) {
            future.complete(records);
        }
    }

    @Test
    public void testConsumer() throws Exception {
        final ConsumerRecord<String, Message> record = new ConsumerRecord<>("topic", 0, 0L, "key", null);
        final CompletableFuture<ConsumerRecords<String, Message>> future = new CompletableFuture<>();
        final TopicPartition topic = new TopicPartition("topic", 0);
        final AtomicBoolean val = new AtomicBoolean(false);
        final MockConsumer<String, Message> consumer = new MockConsumer<>(EARLIEST);

        consumer.updateBeginningOffsets(singletonMap(topic, 0L));
        consumer.schedulePollTask(() -> val.set(true));

        final MyConsumerRunner runner = new MyConsumerRunner(asList(topic), consumer, future);
        new Thread(runner).start();

        consumer.addRecord(record);

        assertTrue(future.get().records(topic).contains(record));
        assertTrue(val.get());

        runner.shutdown();
    }
}
