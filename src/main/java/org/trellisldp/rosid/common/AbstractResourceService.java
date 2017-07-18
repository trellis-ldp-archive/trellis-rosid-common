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

import static java.time.Instant.now;
import static java.util.Collections.singleton;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_INTERNAL_NOTIFICATION;
import static org.trellisldp.rosid.common.RDFUtils.endedAtQuad;
import static org.trellisldp.rosid.common.RDFUtils.getParent;
import static org.trellisldp.vocabulary.AS.Create;
import static org.trellisldp.vocabulary.AS.Delete;
import static org.trellisldp.vocabulary.RDF.type;
import static org.trellisldp.vocabulary.Trellis.PreferAudit;

import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;
import org.trellisldp.spi.EventService;

/**
 * @author acoburn
 */
public abstract class AbstractResourceService extends LockableResourceService {

    private static final Logger LOGGER = getLogger(AbstractResourceService.class);

    protected final String SKOLEM_BNODE_PREFIX = "trellis:bnode/";

    private final NotificationServiceRunner notificationService;

    /**
     * Create an AbstractResourceService with the given properties
     * @param service the event service
     * @param kafkaProperties the kafka properties
     * @param zkProperties the zookeeper properties
     */
    public AbstractResourceService(final EventService service, final Properties kafkaProperties,
            final Properties zkProperties) {
        super(kafkaProperties, zkProperties);
        notificationService = new NotificationServiceRunner(kafkaProperties, service);
        notificationService.subscribe(singleton(TOPIC_INTERNAL_NOTIFICATION));
        new Thread(notificationService).start();
    }

    /**
     * Create an AbstractResourceService with the given producer
     * @param service the event service
     * @param producer the kafka producer
     * @param consumer the kafka consumer
     * @param curator the zookeeper curator
     */
    public AbstractResourceService(final EventService service, final Producer<String, Dataset> producer,
            final Consumer<String, Dataset> consumer, final CuratorFramework curator) {
        super(producer, curator);
        notificationService = new NotificationServiceRunner(consumer, service);
        notificationService.subscribe(singleton(TOPIC_INTERNAL_NOTIFICATION));
        new Thread(notificationService).start();
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
        final InterProcessLock lock = getLock(identifier);

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
    public Optional<IRI> getContainer(final IRI identifier) {
        return getParent(identifier.getIRIString()).map(rdf::createIRI);
    }

    @Override
    public RDFTerm skolemize(final RDFTerm term) {
        if (term instanceof BlankNode) {
            return rdf.createIRI(SKOLEM_BNODE_PREFIX + ((BlankNode) term).uniqueReference());
        }
        return term;
    }

    @Override
    public RDFTerm unskolemize(final RDFTerm term) {
        if (term instanceof IRI) {
            final String iri = ((IRI) term).getIRIString();
            if (iri.startsWith(SKOLEM_BNODE_PREFIX)) {
                return rdf.createBlankNode(iri.substring(SKOLEM_BNODE_PREFIX.length()));
            }
        }
        return term;
    }

    @Override
    public void close() {
        super.close();
        notificationService.shutdown();
    }

    @Override
    public Boolean compact(final IRI identifier) {
        // TODO -- implement this
        return false;
    }

    @Override
    public Boolean purge(final IRI identifier) {
        // TODO -- implement this
        return false;
    }

    @Override
    public Stream<Triple> list(final IRI identifier) {
        // TODO -- implement this
        return empty();
    }

    @Override
    public Stream<Quad> export(final IRI identifier) {
        // TODO -- implement this
        return empty();
    }
}
