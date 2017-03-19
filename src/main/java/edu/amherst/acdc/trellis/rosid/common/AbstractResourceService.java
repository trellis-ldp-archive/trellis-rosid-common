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
package edu.amherst.acdc.trellis.rosid.common;

import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_UPDATE;
import static edu.amherst.acdc.trellis.vocabulary.RDF.type;
import static java.time.Instant.now;
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.Session;
import edu.amherst.acdc.trellis.vocabulary.PROV;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import edu.amherst.acdc.trellis.vocabulary.XSD;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
public abstract class AbstractResourceService implements ResourceService, AutoCloseable {

    private static final Logger LOGGER = getLogger(AbstractResourceService.class);

    protected final Producer<String, Dataset> producer;

    protected EventService evtSvc;

    /**
     * Create an AbstractResourceService
     */
    public AbstractResourceService() {
        this(new KafkaProducer<>(kafkaProducerProps()));
    }

    /**
     * Create an AbstractResourceService with the given producer
     * @param producer the kafka producer
     */
    public AbstractResourceService(final Producer<String, Dataset> producer) {
        this.producer = producer;
    }

    @Override
    public void bind(final EventService svc) {
        LOGGER.info("Binding EventService to RepositoryService");
        evtSvc = svc;
    }

    @Override
    public void unbind(final EventService svc) {
        if (Objects.equals(evtSvc, svc)) {
            LOGGER.info("Unbinding EventService from RepositoryService");
            evtSvc = null;
        }
    }

    @Override
    public Boolean put(final Session session, final IRI identifier, final Dataset dataset) {
        // TODO -- add/remove zk node

        // Add audit quads -- MOVE this to the HTTP layer // Add AS Create/Update/Delete type
        final RDF rdf = RDFUtils.getInstance();
        final BlankNode bnode = rdf.createBlankNode();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, bnode));
        dataset.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, PROV.Activity));
        dataset.add(rdf.createQuad(Trellis.PreferAudit, bnode, PROV.startedAtTime, rdf.createLiteral(now().toString(),
                        XSD.dateTime)));
        dataset.add(rdf.createQuad(Trellis.PreferAudit, bnode, PROV.wasAssociatedWith, session.getAgent()));
        session.getDelegatedBy().ifPresent(delegate ->
                dataset.add(rdf.createQuad(Trellis.PreferAudit, bnode, PROV.actedOnBehalfOf, delegate)));

        try {
            final RecordMetadata res = producer.send(
                    new ProducerRecord<>(TOPIC_UPDATE, identifier.getIRIString(), dataset)).get();
            LOGGER.info("Sent record to topic: {} {}", res.topic(), res.timestamp());
            return true;
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.error("Error sending record to kafka topic: {}", ex.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        // TODO -- close any ZK connections
        producer.close();
    }

    private static Properties kafkaProducerProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("kafka.bootstrap.servers"));
        props.put("acks", System.getProperty("kafka.acks", "all"));
        props.put("retries", System.getProperty("kafka.retries", "0"));
        props.put("batch.size", System.getProperty("kafka.batch.size", "16384"));
        props.put("linger.ms", System.getProperty("kafka.linger.ms", "1"));
        props.put("buffer.memory", System.getProperty("kafka.buffer.memory", "33554432"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "edu.amherst.acdc.trellis.rosid.common.DatasetSerializer");
        return props;
    }
}
