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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_CACHE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_DELETE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_DELETE;
import static org.trellisldp.vocabulary.RDF.type;

import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.PROV;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;

/**
 * @author acoburn
 */
@RunWith(MockitoJUnitRunner.class)
public class EventProducerTest {

    private static final RDF rdf = new JenaRDF();

    private final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private final IRI other1 = rdf.createIRI("trellis:repository/other1");
    private final IRI other2 = rdf.createIRI("trellis:repository/other2");
    private final IRI inbox = rdf.createIRI("http://example.org/inbox");
    private final IRI subject = rdf.createIRI("http://example.org/subject");

    private final MockProducer<String, String> producer = new MockProducer<>(true,
            new StringSerializer(), new StringSerializer());

    @Mock
    private Producer<String, String> mockProducer;

    @Mock
    private Future<RecordMetadata> mockFuture;

    @Test
    public void testEventProducer() throws Exception {
        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, false);
        event.into(existing.stream());

        assertTrue(event.emit());
        assertEquals(4L, event.getRemoved().count());
        assertEquals(5L, event.getAdded().count());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(0L, records.size());
    }

    @Test
    public void testEventCreation() throws Exception {
        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Create));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified);
        event.into(existing.stream());

        assertEquals(4L, event.getRemoved().count());
        assertEquals(6L, event.getAdded().count());
        assertTrue(event.emit());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(2L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_CONTAINMENT_ADD)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_MEMBERSHIP_ADD)).count());
    }

    @Test
    public void testEventDeletion() throws Exception {
        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        final BlankNode bnode = rdf.createBlankNode();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, AS.Delete));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, true);
        event.into(existing.stream());

        assertEquals(4L, event.getRemoved().count());
        assertEquals(6L, event.getAdded().count());
        assertTrue(event.emit());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(3L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_CONTAINMENT_DELETE)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_MEMBERSHIP_DELETE)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_CACHE)).count());
    }

    @Test
    public void testProducerFailure() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(new InterruptedException("Interrupted exception!"));

        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        final BlankNode bnode = rdf.createBlankNode();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, AS.Delete));

        final EventProducer event = new EventProducer(mockProducer, identifier, modified, true);
        event.into(existing.stream());
        assertFalse(event.emit());
    }
}
