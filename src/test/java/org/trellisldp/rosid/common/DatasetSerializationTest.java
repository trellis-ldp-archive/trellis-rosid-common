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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;

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
public class DatasetSerializationTest {

    private static final RDF rdf = new JenaRDF();

    private final DatasetSerialization serializer = new DatasetSerialization();
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
        final Dataset data = serializer.deserialize("topic", serializer.serialize("topic", dataset));
        assertTrue(data.contains(title));
        assertTrue(data.contains(description));
        assertTrue(data.contains(subject));
        assertEquals(3L, data.size());
    }

    @Test
    public void testDeserialization() {
        final String data = "" +
            "<trellis:repository/resource> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
            "<http://www.w3.org/ns/ldp#Container> <http://www.trellisldp.org/ns/trellis#PreferServerManaged> .\n" +
            "<trellis:repository/resource> <http://purl.org/dc/terms/title> " +
            "\"A title\"@eng <http://www.trellisldp.org/ns/trellis#PreferUserManaged> .\n" +
            "<trellis:repository/resource> <http://purl.org/dc/terms/description> " +
            "\"A longer description\"@eng <http://www.trellisldp.org/ns/trellis#PreferUserManaged> .\n" +
            "<trellis:repository/resource> <http://purl.org/dc/terms/subject> " +
            "<http://example.org/subject/1> <http://www.trellisldp.org/ns/trellis#PreferUserManaged> .\n";
        final Dataset msg = serializer.deserialize("topic", data.getBytes(UTF_8));
        assertTrue(msg.contains(title));
        assertTrue(msg.contains(description));
        assertTrue(msg.contains(subject));
        assertTrue(msg.contains(ixmodel));
        assertEquals(4L, msg.size());
    }

    @Test
    public void testSimpleSerialization() {
        assertEquals(0L, serializer.serialize("topic", null).length);
        assertEquals(0L, serializer.serialize("topic", rdf.createDataset()).length);
    }

    @Test
    public void testSimpleDeserialization() {
        assertEquals(0L, serializer.deserialize("topic", null).size());
        assertEquals(0L, serializer.deserialize("topic", new byte[0]).size());
    }
}
