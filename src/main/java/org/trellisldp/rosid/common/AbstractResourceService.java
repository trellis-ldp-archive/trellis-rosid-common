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
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.common.RDFUtils.endedAtQuad;
import static org.trellisldp.rosid.common.RDFUtils.getParent;
import static org.trellisldp.spi.RDFUtils.TRELLIS_BNODE_PREFIX;
import static org.trellisldp.spi.RDFUtils.toExternalTerm;
import static org.trellisldp.vocabulary.AS.Create;
import static org.trellisldp.vocabulary.AS.Delete;
import static org.trellisldp.vocabulary.RDF.type;
import static org.trellisldp.vocabulary.Trellis.PreferAudit;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;
import org.trellisldp.spi.EventService;

/**
 * @author acoburn
 */
public abstract class AbstractResourceService extends LockableResourceService {

    private static final Logger LOGGER = getLogger(AbstractResourceService.class);

    private final Supplier<String> idSupplier;

    protected final Boolean async;

    protected final EventService notifications;

    protected final Map<String, String> partitions;

    /**
     * Create an AbstractResourceService with the given producer
     * @param partitions the partitions
     * @param producer the kafka producer
     * @param curator the zookeeper curator
     * @param notifications the event service
     * @param idSupplier a supplier of new identifiers
     * @param async write cached resources asynchronously if true, synchronously if false
     */
    public AbstractResourceService(final Map<String, String> partitions, final Producer<String, String> producer,
            final CuratorFramework curator, final EventService notifications, final Supplier<String> idSupplier,
            final Boolean async) {
        super(producer, curator);

        requireNonNull(partitions, "partition configuration may not be null!");

        this.partitions = partitions;
        this.notifications = notifications;
        this.async = async;
        this.idSupplier = idSupplier;
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

        if (status && nonNull(notifications)) {
            final String baseUrl = partitions.get(identifier.getIRIString().split(":", 2)[1].split("/")[0]);
            notifications.emit(new Notification(toExternalTerm(identifier, baseUrl).getIRIString(), dataset));
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

        final EventProducer eventProducer = new EventProducer(producer, identifier, dataset, async);
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
            return rdf.createIRI(TRELLIS_BNODE_PREFIX + ((BlankNode) term).uniqueReference());
        }
        return term;
    }

    @Override
    public RDFTerm unskolemize(final RDFTerm term) {
        if (term instanceof IRI) {
            final String iri = ((IRI) term).getIRIString();
            if (iri.startsWith(TRELLIS_BNODE_PREFIX)) {
                return rdf.createBlankNode(iri.substring(TRELLIS_BNODE_PREFIX.length()));
            }
        }
        return term;
    }

    @Override
    public Stream<Quad> export(final String partition, final Collection<IRI> graphNames) {
        return list(partition).map(Triple::getSubject).map(x -> (IRI) x)
            // TODO - JDK9 optional to stream
            .flatMap(id -> get(id).map(Stream::of).orElseGet(Stream::empty))
            .flatMap(resource -> resource.stream(graphNames).map(q ->
                        rdf.createQuad(resource.getIdentifier(), q.getSubject(), q.getPredicate(), q.getObject())));
    }

    @Override
    public Supplier<String> getIdentifierSupplier() {
        return idSupplier;
    }
}
