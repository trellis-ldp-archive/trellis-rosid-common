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

import static java.util.Objects.nonNull;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.riot.RDFDataMgr.write;
import static org.apache.jena.sparql.core.DatasetGraphFactory.create;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author acoburn
 */
public class DatasetSerialization implements Serializer<Dataset>, Deserializer<Dataset> {

    private static final JenaRDF rdf = new JenaRDF();

    @Override
    public void configure(final Map<String, ?> map, final boolean isKey) {
    }

    @Override
    public byte[] serialize(final String topic, final Dataset dataset) {
        if (nonNull(dataset)) {
            final DatasetGraph datasetGraph = create();
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            dataset.stream().map(quad -> rdf.asJenaQuad(quad)).forEach(datasetGraph::add);
            write(os, datasetGraph, NQUADS);
            return os.toByteArray();
        }
        return new byte[0];
    }

    @Override
    public Dataset deserialize(final String topic, final byte[] data) {
        final DatasetGraph dataset = create();
        if (nonNull(data)) {
            read(dataset, new ByteArrayInputStream(data), null, NQUADS);
        }
        return rdf.asDataset(dataset);
    }

    @Override
    public void close() {
    }
}
