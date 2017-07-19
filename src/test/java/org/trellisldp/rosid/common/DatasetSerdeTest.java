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

import static java.util.Collections.emptyMap;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.trellisldp.vocabulary.RDF.type;
import static org.trellisldp.spi.RDFUtils.getInstance;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
public class DatasetSerdeTest {

    private static final RDF rdf = getInstance();

    private final IRI identifier = rdf.createIRI("trellis:repository/resource");

    @Test
    public void testStructure() {
        final DatasetSerde serde = new DatasetSerde();
        serde.configure(emptyMap(), false);
        assertTrue(serde.serializer() instanceof Serializer);
        assertTrue(serde.deserializer() instanceof Deserializer);
        serde.close();
    }

    @Test
    public void testRoundTrip() {
        final Dataset dataset = rdf.createDataset();
        final DatasetSerde serde = new DatasetSerde();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        final byte[] serialized = serde.serializer().serialize("Topic", dataset);
        final Dataset other = serde.deserializer().deserialize("Topic", serialized);
        assertEquals(2L, other.size());
        assertTrue(other.contains(of(Trellis.PreferUserManaged), identifier, DC.title, rdf.createLiteral("A title")));
        assertTrue(other.contains(of(Trellis.PreferServerManaged), identifier, type, LDP.Container));
    }
}
