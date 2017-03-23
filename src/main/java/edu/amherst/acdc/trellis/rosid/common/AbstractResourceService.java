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

import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_CACHE;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_INBOUND_ADD;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_INBOUND_DELETE;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_LDP_CONTAINMENT_ADD;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_LDP_CONTAINMENT_DELETE;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_LDP_MEMBERSHIP_ADD;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_LDP_MEMBERSHIP_DELETE;
import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.endedAtQuad;
import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.getInstance;
import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.getParent;
import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.inDomain;
import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.objectIsSameResource;
import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.subjectIsSameResource;
import static edu.amherst.acdc.trellis.vocabulary.AS.Create;
import static edu.amherst.acdc.trellis.vocabulary.AS.Delete;
import static edu.amherst.acdc.trellis.vocabulary.Fedora.PreferInboundReferences;
import static edu.amherst.acdc.trellis.vocabulary.LDP.PreferContainment;
import static edu.amherst.acdc.trellis.vocabulary.LDP.contains;
import static edu.amherst.acdc.trellis.vocabulary.RDF.type;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.PreferAudit;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.PreferServerManaged;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.PreferUserManaged;
import static java.time.Instant.now;
import static java.util.Collections.emptyMap;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Stream.concat;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.RuntimeRepositoryException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
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

    private final String SESSION_ZNODE = "/session";

    protected final Producer<String, Dataset> producer;

    protected final CuratorFramework curator;

    protected EventService evtSvc;

    protected RDF rdf = getInstance();

    /**
     * Create an AbstractResourceService
     */
    public AbstractResourceService() {
        this(new KafkaProducer<>(kafkaProducerProps()),
                newClient(System.getProperty("zk.connectString"),
                    new BoundedExponentialBackoffRetry(
                        Integer.parseInt(System.getProperty("zk.retry.ms", "2000")),
                        Integer.parseInt(System.getProperty("zk.retry.max.ms", "30000")),
                        Integer.parseInt(System.getProperty("zk.retry.max", "10")))));
    }

    /**
     * Create an AbstractResourceService with the given producer
     * @param producer the kafka producer
     * @param curator the zookeeper curator
     */
    public AbstractResourceService(final Producer<String, Dataset> producer, final CuratorFramework curator) {
        this.producer = producer;
        this.curator = curator;
        this.curator.start();
        try {
            curator.createContainers(SESSION_ZNODE);
        } catch (final Exception ex) {
            LOGGER.error("Could not create zk session node: {}", ex.getMessage());
            throw new RuntimeRepositoryException(ex);
        }
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

    /**
     * Write to the persistence layer
     * @param identifier the identifier
     * @param delete the quads to delete
     * @param add the quads to add
     * @param time the time the resource is written
     * @return true if the write was successful; false otherwise
     */
    protected abstract Boolean write(final IRI identifier, final Stream<? extends Quad> delete,
            final Stream<? extends Quad> add, final Instant time);

    @Override
    public Boolean put(final IRI identifier, final Dataset dataset) {
        final String path = SESSION_ZNODE + "/" + md5Hex(identifier.getIRIString());
        final InterProcessLock lock = new InterProcessSemaphoreMutex(curator, path);

        try {
            if (!lock.acquire(Long.parseLong(System.getProperty("zk.lock.wait.ms", "100")), MILLISECONDS)) {
                return false;
            }
        } catch (final Exception ex) {
            LOGGER.error("Error acquiring resource lock: {}", ex.getMessage());
            return false;
        }

        final Boolean status = doWrite(identifier, dataset);

        try {
            lock.release();
        } catch (final Exception ex) {
            LOGGER.error("Error releasing resource lock: {}", ex.getMessage());
        }

        return status;
    }

    /**
     * Write the resource data to the persistence layer
     * @param identifier the identifier
     * @param dataset the dataset
     * @return true if the operation was successful; false otherwise
     */
    private Boolean doWrite(final IRI identifier, final Dataset dataset) {
        final Instant time = now();
        final Boolean isCreate = dataset.contains(of(PreferAudit), null, type, Create);
        final Boolean isDelete = dataset.contains(of(PreferAudit), null, type, Delete);
        final Optional<Resource> resource = get(identifier, time);

        if (resource.isPresent() && isCreate) {
            LOGGER.warn("The resource already exists and cannot be created: {}", identifier.getIRIString());
            return false;
        } else if (!resource.isPresent() && isDelete) {
            LOGGER.warn("The resource does not exist and cannot be deleted: {}", identifier.getIRIString());
            return false;
        }

        final Dataset existing = rdf.createDataset();
        resource.ifPresent(res -> res.stream().filter(q -> q.getGraphName().isPresent() &&
                (PreferUserManaged.equals(q.getGraphName().get()) ||
                PreferServerManaged.equals(q.getGraphName().get()))).forEach(existing::add));

        final Dataset adding = rdf.createDataset();
        dataset.stream().filter(q -> !existing.contains(q)).forEach(adding::add);

        final Dataset removing = rdf.createDataset();
        existing.stream().filter(q -> !dataset.contains(q)).forEach(removing::add);

        final Instant later = now();
        if (!write(identifier, removing.stream(), concat(adding.stream(), endedAtQuad(identifier, adding, later)),
                    later)) {
            LOGGER.error("Could not write data to persistence layer!");
            return false;
        }

        return emit(identifier, dataset, adding, removing);
    }

    /**
     * Emit messages to the appropriate Kafka topics
     * @param identifier the identifier
     * @param dataset the original dataset
     * @param adding the dataset of quads being added
     * @param removing the dataset of quads being removed
     * @return true if all messages are successfully added to the Kafka broker; false otherwise
     */
    private Boolean emit(final IRI identifier, final Dataset dataset, final Dataset adding, final Dataset removing) {
        final String domain = identifier.getIRIString().split("/", 2)[0];
        final Boolean isCreate = dataset.contains(of(PreferAudit), null, type, Create);
        final Boolean isDelete = dataset.contains(of(PreferAudit), null, type, Delete);
        try {
            final List<Future<RecordMetadata>> results = new ArrayList<>();

            results.add(producer.send(new ProducerRecord<>(TOPIC_CACHE, identifier.getIRIString(), dataset)));

            // Handle the addition of any in-domain outbound triples
            adding.getGraph(PreferUserManaged).map(g -> g.stream().filter(subjectIsSameResource(identifier))
                .filter(inDomain(domain).and(objectIsSameResource(identifier).negate()))
                .map(t -> rdf.createQuad(PreferInboundReferences, t.getSubject(), t.getPredicate(), t.getObject()))
                .collect(groupingBy(q -> ((IRI) q.getObject()).getIRIString()))).orElse(emptyMap())
                .entrySet().forEach(e -> {
                    final Dataset data = rdf.createDataset();
                    e.getValue().forEach(data::add);
                    results.add(producer.send(new ProducerRecord<>(TOPIC_INBOUND_ADD, e.getKey(), data)));
                });

            // Handle the removal of any in-domain outbound triples
            removing.getGraph(PreferUserManaged).map(g -> g.stream().filter(subjectIsSameResource(identifier))
                .filter(inDomain(domain).and(objectIsSameResource(identifier).negate()))
                .map(t -> rdf.createQuad(PreferInboundReferences, t.getSubject(), t.getPredicate(), t.getObject()))
                .collect(groupingBy(q -> ((IRI) q.getObject()).getIRIString()))).orElse(emptyMap())
                .entrySet().forEach(e -> {
                    final Dataset data = rdf.createDataset();
                    e.getValue().forEach(data::add);
                    results.add(producer.send(new ProducerRecord<>(TOPIC_INBOUND_DELETE, e.getKey(), data)));
                });

            // Update the containment triples of the parent resource if this is a delete or create operation
            getParent(identifier.getIRIString()).ifPresent(container -> {
                dataset.add(rdf.createQuad(PreferContainment, rdf.createIRI(container), contains, identifier));
                if (isDelete) {
                    results.add(producer.send(new ProducerRecord<>(TOPIC_LDP_CONTAINMENT_DELETE, container, dataset)));
                    results.add(producer.send(new ProducerRecord<>(TOPIC_LDP_MEMBERSHIP_DELETE, container, dataset)));
                } else if (isCreate) {
                    results.add(producer.send(new ProducerRecord<>(TOPIC_LDP_CONTAINMENT_ADD, container, dataset)));
                    results.add(producer.send(new ProducerRecord<>(TOPIC_LDP_MEMBERSHIP_ADD, container, dataset)));
                }
            });

            for (final Future<RecordMetadata> result : results) {
                final RecordMetadata res = result.get();
                LOGGER.info("Send record to topic: {}, {}", res.topic(), res.timestamp());
            }

            return true;
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.error("Error sending record to kafka topic: {}", ex.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        producer.close();
        curator.close();
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
