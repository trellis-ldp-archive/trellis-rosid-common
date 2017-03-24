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

import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.endedAtQuad;
import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.getInstance;
import static edu.amherst.acdc.trellis.vocabulary.AS.Create;
import static edu.amherst.acdc.trellis.vocabulary.AS.Delete;
import static edu.amherst.acdc.trellis.vocabulary.RDF.type;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.PreferAudit;
import static java.time.Instant.now;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Stream.concat;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.apache.curator.framework.imps.CuratorFrameworkState.LATENT;
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.RuntimeRepositoryException;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
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
        if (LATENT.equals(curator.getState())) {
            this.curator.start();
        }
        try {
            this.curator.createContainers(SESSION_ZNODE);
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

        final Boolean status = tryWrite(identifier, dataset);

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
    private Boolean tryWrite(final IRI identifier, final Dataset dataset) {
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

        final EventProducer eventProducer = new EventProducer(producer, identifier, dataset);
        resource.map(Resource::stream).ifPresent(eventProducer::into);

        final Instant later = now();
        if (!write(identifier, eventProducer.getRemoved(),
                    concat(eventProducer.getAdded(), endedAtQuad(identifier, dataset, later)), later)) {
            LOGGER.error("Could not write data to persistence layer!");
            return false;
        }

        return eventProducer.emit();
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
