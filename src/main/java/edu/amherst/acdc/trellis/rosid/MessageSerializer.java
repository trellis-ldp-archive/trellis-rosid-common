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

import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.nonNull;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.riot.RDFDataMgr.write;
import static org.apache.jena.sparql.core.DatasetGraphFactory.create;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author acoburn
 */
public class MessageSerializer implements Serializer<Message>, Deserializer<Message> {

    private static final JenaRDF rdf = new JenaRDF();

    @Override
    public void configure(final Map<String, ?> map, final boolean isKey) {
    }

    @Override
    public byte[] serialize(final String topic, final Message data) {
        if (nonNull(data.getDataset())) {
            final StringWriter writer = new StringWriter();
            final DatasetGraph dataset = create();
            data.getDataset().stream().map(quad -> rdf.asJenaQuad(quad)).forEach(dataset::add);
            write(writer, dataset, NQUADS);
            return join(",", data.getIdentifier().getIRIString(), writer.toString()).getBytes(UTF_8);
        } else {
            return data.getIdentifier().getIRIString().getBytes(UTF_8);
        }
    }

    @Override
    public Message deserialize(final String topic, final byte[] data) {
        final String[] parts = new String(data, UTF_8).split(",", 2);
        final DatasetGraph dataset = create();
        if (parts.length == 1) {
            return new Message(rdf.createIRI(parts[0]), null);
        } else {
            read(dataset, new StringReader(parts[1]), null, NQUADS);
            return new Message(rdf.createIRI(parts[0]), rdf.asDataset(dataset));
        }
    }

    @Override
    public void close() {
    }
}
