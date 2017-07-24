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

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.trellisldp.vocabulary.RDF.type;

import org.trellisldp.api.Resource;
import org.trellisldp.spi.EventService;
import org.trellisldp.spi.ResourceService;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractResourceServiceTest {

    private static final RDF rdf = new JenaRDF();
    private static final IRI existing = rdf.createIRI("trellis:repository/existing");
    private static final IRI unwritable = rdf.createIRI("trellis:repository/unwritable");
    private static final IRI resource = rdf.createIRI("trellis:repository/resource");

    private static TestingServer curator;

    @Mock
    private EventService mockEventService, mockEventService2;

    @Mock
    private static Resource mockResource;

    public static class MyResourceService extends AbstractResourceService {

        public MyResourceService(final EventService eventService, final String connectString) {
            super(eventService, new MockProducer<>(true, new StringSerializer(), new DatasetSerialization()),
                    new MockConsumer<>(EARLIEST),
                    getZkClient(connectString));
        }

        private static CuratorFramework getZkClient(final String connectString) {
            final CuratorFramework zk = newClient(connectString, new RetryNTimes(10, 1000));
            zk.start();
            return zk;
        }

        @Override
        public Optional<Resource> get(final IRI identifier) {
            if (identifier.equals(existing)) {
                return of(mockResource);
            }
            return empty();
        }

        @Override
        public Optional<Resource> get(final IRI identifier, final Instant time) {
            if (identifier.equals(existing)) {
                return of(mockResource);
            }
            return empty();
        }

        @Override
        public Boolean write(final IRI identifier, final Stream<? extends Quad> delete,
                final Stream<? extends Quad> add, final Instant time) {
            return !identifier.equals(unwritable);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        curator = new TestingServer(true);
    }

    @Test
    public void testSkolemization() {
        final BlankNode bnode = rdf.createBlankNode("testing");
        final IRI iri = rdf.createIRI("trellis:bnode/testing");
        final IRI root = rdf.createIRI("trellis:repository");
        final IRI child = rdf.createIRI("trellis:repository/resource/child");
        final ResourceService svc = new MyResourceService(mockEventService, curator.getConnectString());

        assertTrue(svc.skolemize(bnode) instanceof IRI);
        assertTrue(((IRI) svc.skolemize(bnode)).getIRIString().startsWith("trellis:bnode/"));
        assertTrue(svc.unskolemize(iri) instanceof BlankNode);
        assertEquals(svc.unskolemize(iri), svc.unskolemize(iri));

        assertFalse(svc.unskolemize(rdf.createLiteral("Test")) instanceof BlankNode);
        assertFalse(svc.unskolemize(resource) instanceof BlankNode);
        assertFalse(svc.skolemize(rdf.createLiteral("Test2")) instanceof IRI);
        assertEquals(of(resource), svc.getContainer(child));
        assertEquals(of(root), svc.getContainer(resource));
        assertEquals(empty(), svc.getContainer(root));
    }

    @Test
    public void testPutCreate() {
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Create));

        try (final MyResourceService svc = new MyResourceService(mockEventService, curator.getConnectString())) {
            assertTrue(svc.put(resource, dataset));
            assertFalse(svc.put(existing, dataset));
            assertFalse(svc.put(unwritable, dataset));
        }
    }

    @Test
    public void testPutDelete() {
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Delete));

        try (final MyResourceService svc = new MyResourceService(mockEventService, curator.getConnectString())) {
            assertFalse(svc.put(resource, dataset));
            assertTrue(svc.put(existing, dataset));
            assertFalse(svc.put(unwritable, dataset));
        }
    }
}
