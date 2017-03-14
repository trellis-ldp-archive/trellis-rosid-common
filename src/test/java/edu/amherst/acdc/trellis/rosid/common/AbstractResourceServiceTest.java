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

import static java.time.Instant.parse;
import static java.util.Optional.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.Session;

import java.time.Instant;
import java.util.Optional;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.MockProducer;
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

    @Mock
    private Session mockSession;

    @Mock
    private EventService mockEventService, mockEventService2;

    public static class MyResourceService extends AbstractResourceService {
        public MyResourceService() {
            super(new MockProducer<>(true, new StringSerializer(), new MessageSerializer()));
        }

        @Override
        public Optional<Resource> get(final Session session, final IRI identifier) {
            return empty();
        }

        @Override
        public Optional<Resource> get(final Session session, final IRI identifier, final Instant time) {
            return empty();
        }
    }

    @Test
    public void testResourceService() {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final ResourceService svc = new MyResourceService();
        svc.bind(mockEventService);
        svc.unbind(mockEventService);
        svc.bind(mockEventService);
        svc.unbind(mockEventService2);
        assertFalse(svc.exists(mockSession, identifier, time));
        assertTrue(svc.put(mockSession, identifier, rdf.createDataset()));
        assertTrue(svc.delete(mockSession, identifier));
    }
}
