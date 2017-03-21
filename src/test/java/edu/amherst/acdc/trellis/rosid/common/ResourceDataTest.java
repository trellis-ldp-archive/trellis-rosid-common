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

import static edu.amherst.acdc.trellis.vocabulary.RDF.type;
import static java.time.Instant.now;
import static java.time.Instant.parse;
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
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import edu.amherst.acdc.trellis.vocabulary.XSD;
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
        assertEquals(parse("2017-02-05T09:31:12Z"), res.created);
        assertEquals("file:/path/to/datastream", res.datastream.id);
        assertEquals("image/jpeg", res.datastream.format);
        assertEquals(new Long(103527L), res.datastream.size);
        assertNull(res.insertedContentRelation);
    }

    @Test
    public void testRDFSource() {
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final IRI inbox = rdf.createIRI("http://example.com/receiver/inbox");
        final Instant time = now();
        final Literal created = rdf.createLiteral(time.toString(), XSD.dateTime);
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.created, created));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified, created));

        final Optional<ResourceData> rd = ResourceData.from(identifier, dataset);
        assertTrue(rd.isPresent());
        rd.ifPresent(data -> {
            assertEquals(identifier.getIRIString(), data.id);
            assertEquals(inbox.getIRIString(), data.inbox);
            assertEquals(LDP.RDFSource.getIRIString(), data.ldpType);
            assertEquals(time, data.created);
            assertEquals(time, data.modified);
        });
    }

    @Test
    public void testNonRDFSource() {
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final IRI datastream = rdf.createIRI("file://path/to/resource");
        final IRI inbox = rdf.createIRI("http://example.com/receiver/inbox");
        final Literal format = rdf.createLiteral("image/jpeg");
        final Literal extent = rdf.createLiteral("12345", XSD.long_);
        final Instant time = now();
        final Literal created = rdf.createLiteral(time.toString(), XSD.dateTime);
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.NonRDFSource));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.created, created));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified, created));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.hasPart, datastream));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, datastream, DC.created, created));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, datastream, DC.modified, created));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, datastream, DC.format, format));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, datastream, DC.extent, extent));


        final Optional<ResourceData> rd = ResourceData.from(identifier, dataset);
        assertTrue(rd.isPresent());
        rd.ifPresent(data -> {
            assertEquals(identifier.getIRIString(), data.id);
            assertEquals(inbox.getIRIString(), data.inbox);
            assertEquals(LDP.NonRDFSource.getIRIString(), data.ldpType);
            assertEquals(time, data.created);
            assertEquals(time, data.modified);
            assertNotNull(data.datastream);
            assertEquals(datastream.getIRIString(), data.datastream.id);
            assertEquals(12345L, (long) data.datastream.size);
            assertEquals(format.getLexicalForm(), data.datastream.format);
            assertEquals(time, data.datastream.created);
            assertEquals(time, data.datastream.modified);
        });
    }
}
