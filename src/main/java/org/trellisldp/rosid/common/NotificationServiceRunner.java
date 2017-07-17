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

import static java.util.Collections.singleton;

import java.util.Properties;

import org.apache.commons.rdf.api.Dataset;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.trellisldp.spi.EventService;

/**
 * @author acoburn
 */
class NotificationServiceRunner extends AbstractConsumerRunner {

    private final EventService service;

    public NotificationServiceRunner(final TopicPartition topic, final Properties props, final EventService service) {
        super(singleton(topic), props);
        this.service = service;
    }

    @Override
    protected void handleRecords(final ConsumerRecords<String, Dataset> records) {
        records.iterator().forEachRemaining(record -> {
            service.emit(new Notification(record.key(), record.value()));
        });
    }
}
