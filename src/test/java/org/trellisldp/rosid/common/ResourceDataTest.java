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

import static org.trellisldp.vocabulary.RDF.type;
import static java.time.Instant.now;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.Test;

/**
 * @author acoburn
 */
public class ResourceDataTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final RDF rdf = new JenaRDF();

    static {
        MAPPER.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    @Test
    public void testDeserialize() throws IOException {
        final ResourceData res = MAPPER.readValue(getClass().getResourceAsStream("/resource1.json"),
                ResourceData.class);

        assertEquals("trellis:repository/resource1", res.id);
        assertEquals(LDP.Container.getIRIString(), res.ldpType);
        assertTrue(res.userTypes.contains("http://example.org/ns/CustomType"));
        assertEquals("http://receiver.example.org/inbox", res.inbox);
        assertEquals("trellis:repository/acl/public", res.accessControl);
        assertEquals("file:/path/to/binary", res.binary.id);
        assertEquals("image/jpeg", res.binary.format);
        assertEquals(new Long(103527L), res.binary.size);
        assertNull(res.insertedContentRelation);
    }

    @Test
    public void testRDFSource() {
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final IRI inbox = rdf.createIRI("http://example.com/receiver/inbox");
        final Instant time = now();
        final Literal modified = rdf.createLiteral(time.toString(), XSD.dateTime);
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified, modified));

        final Optional<ResourceData> rd = ResourceData.from(identifier, dataset);
        assertTrue(rd.isPresent());
        rd.ifPresent(data -> {
            assertEquals(identifier.getIRIString(), data.id);
            assertEquals(inbox.getIRIString(), data.inbox);
            assertEquals(LDP.RDFSource.getIRIString(), data.ldpType);
            assertEquals(time, data.modified);
        });
    }

    @Test
    public void testNonRDFSource() {
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final IRI binary = rdf.createIRI("file://path/to/resource");
        final IRI inbox = rdf.createIRI("http://example.com/receiver/inbox");
        final Literal format = rdf.createLiteral("image/jpeg");
        final Literal extent = rdf.createLiteral("12345", XSD.long_);
        final Instant time = now();
        final Literal modified = rdf.createLiteral(time.toString(), XSD.dateTime);
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.NonRDFSource));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified, modified));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.hasPart, binary));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, binary, DC.modified, modified));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, binary, DC.format, format));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, binary, DC.extent, extent));


        final Optional<ResourceData> rd = ResourceData.from(identifier, dataset);
        assertTrue(rd.isPresent());
        rd.ifPresent(data -> {
            assertEquals(identifier.getIRIString(), data.id);
            assertEquals(inbox.getIRIString(), data.inbox);
            assertEquals(LDP.NonRDFSource.getIRIString(), data.ldpType);
            assertEquals(time, data.modified);
            assertNotNull(data.binary);
            assertEquals(binary.getIRIString(), data.binary.id);
            assertEquals(12345L, (long) data.binary.size);
            assertEquals(format.getLexicalForm(), data.binary.format);
            assertEquals(time, data.binary.modified);
        });
    }
}
