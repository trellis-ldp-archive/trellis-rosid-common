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
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.trellisldp.api.Resource;
import org.trellisldp.spi.EventService;
import org.trellisldp.spi.ResourceService;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
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

/**
 * @author acoburn
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractResourceServiceTest {

    private static final RDF rdf = new JenaRDF();

    private static TestingServer curator;

    @Mock
    private EventService mockEventService, mockEventService2;

    public static class MyResourceService extends AbstractResourceService {
        public MyResourceService(final EventService eventService, final String connectString) {
            super(eventService, new MockProducer<>(true, new StringSerializer(), new DatasetSerialization()),
                    new MockConsumer<>(EARLIEST),
                    newClient(connectString, new RetryNTimes(10, 1000)));
        }

        @Override
        public Optional<Resource> get(final IRI identifier) {
            return empty();
        }

        @Override
        public Optional<Resource> get(final IRI identifier, final Instant time) {
            return empty();
        }

        @Override
        public Boolean write(final IRI identifier, final Stream<? extends Quad> delete,
                final Stream<? extends Quad> add, final Instant time) {
            return true;
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
        final ResourceService svc = new MyResourceService(mockEventService, curator.getConnectString());

        assertTrue(svc.skolemize(bnode) instanceof IRI);
        assertTrue(((IRI) svc.skolemize(bnode)).getIRIString().startsWith("trellis:bnode/"));
        assertTrue(svc.unskolemize(iri) instanceof BlankNode);
        assertEquals(svc.unskolemize(iri), svc.unskolemize(iri));

        assertFalse(svc.unskolemize(rdf.createLiteral("Test")) instanceof BlankNode);
        assertFalse(svc.unskolemize(rdf.createIRI("trellis:repository/resource")) instanceof BlankNode);
        assertFalse(svc.skolemize(rdf.createLiteral("Test2")) instanceof IRI);
    }
}
