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
package edu.amherst.acdc.trellis.rosid;

import static edu.amherst.acdc.trellis.vocabulary.RDF.type;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.Trellis;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.Before;
import org.junit.Test;

/**
 * @author acoburn
 */
public class MessageSerializerTest {

    private static final RDF rdf = new JenaRDF();

    private final MessageSerializer serializer = new MessageSerializer();
    private final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private final Quad title = rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
            rdf.createLiteral("A title", "eng"));
    private final Quad description = rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.description,
            rdf.createLiteral("A longer description", "eng"));
    private final Quad subject = rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject,
            rdf.createIRI("http://example.org/subject/1"));
    private final Quad ixmodel = rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container);

    private final Dataset dataset = rdf.createDataset();

    @Before
    public void setUp() {
        dataset.clear();
        dataset.add(title);
        dataset.add(description);
        dataset.add(subject);
    }

    @Test
    public void testSerialization() {
        final Message msg = new Message(identifier, dataset);
        final Message msg2 = serializer.deserialize("topic", serializer.serialize("topic", msg));
        assertEquals(identifier, msg2.getIdentifier());
        assertTrue(msg2.getDataset().contains(title));
        assertTrue(msg2.getDataset().contains(description));
        assertTrue(msg2.getDataset().contains(subject));
        assertEquals(3L, msg2.getDataset().size());
    }

    @Test
    public void testDeserialization() {
        final String data = "trellis:repository/resource," +
            "<trellis:repository/resource> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
            "<http://www.w3.org/ns/ldp#Container> <http://acdc.amherst.edu/ns/trellis#PreferServerManaged> .\n" +
            "<trellis:repository/resource> <http://purl.org/dc/terms/title> " +
            "\"A title\"@eng <http://acdc.amherst.edu/ns/trellis#PreferUserManaged> .\n" +
            "<trellis:repository/resource> <http://purl.org/dc/terms/description> " +
            "\"A longer description\"@eng <http://acdc.amherst.edu/ns/trellis#PreferUserManaged> .\n" +
            "<trellis:repository/resource> <http://purl.org/dc/terms/subject> " +
            "<http://example.org/subject/1> <http://acdc.amherst.edu/ns/trellis#PreferUserManaged> .\n";
        final Message msg = serializer.deserialize("topic", data.getBytes(UTF_8));
        assertEquals(identifier, msg.getIdentifier());
        assertTrue(msg.getDataset().contains(title));
        assertTrue(msg.getDataset().contains(description));
        assertTrue(msg.getDataset().contains(subject));
        assertTrue(msg.getDataset().contains(ixmodel));
        assertEquals(4L, msg.getDataset().size());
    }

    @Test
    public void testSimpleSerialization() {
        final Message msg = new Message(identifier, null);
        final String data = new String(serializer.serialize("topic", msg));
        assertEquals("trellis:repository/resource", data);
    }

    @Test
    public void testSimpleDeserialization() {
        final String data = "trellis:repository/resource";
        final Message msg = serializer.deserialize("topic", data.getBytes(UTF_8));
        assertEquals(identifier, msg.getIdentifier());
        assertNull(msg.getDataset());
    }
}
