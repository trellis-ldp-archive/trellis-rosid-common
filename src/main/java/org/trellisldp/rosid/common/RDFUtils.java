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
import static java.util.Optional.of;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.riot.RDFDataMgr.write;
import static org.apache.jena.sparql.core.DatasetGraphFactory.create;
import static org.trellisldp.vocabulary.PROV.endedAtTime;
import static org.trellisldp.vocabulary.PROV.wasGeneratedBy;
import static org.trellisldp.vocabulary.Trellis.PreferAudit;
import static org.trellisldp.vocabulary.XSD.dateTime;

import java.time.Instant;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.sparql.core.DatasetGraph;

/**
 * @author acoburn
 */
final class RDFUtils {

    private static final JenaRDF rdf = new JenaRDF();

    /**
     * Get the PROV.endedAtTime quad, wrapped in a Stream
     * @param identifier the identifier
     * @param dataset the dataset
     * @param time the time
     * @return the quad
     */
    public static Stream<Quad> endedAtQuad(final IRI identifier, final Dataset dataset, final Instant time) {
        return dataset.stream(of(PreferAudit), identifier, wasGeneratedBy, null)
            .map(Quad::getObject).filter(term -> term instanceof BlankNodeOrIRI)
            .map(term -> (BlankNodeOrIRI) term)
            .map(term -> ((RDF)rdf).createQuad(PreferAudit, term, endedAtTime,
                    ((RDF)rdf).createLiteral(time.toString(), dateTime))).limit(1);
    }

    /**
     * A predicate that returns true if the object of the provided quad is an IRI
     */
    public static final Predicate<Quad> hasObjectIRI = quad -> quad.getObject() instanceof IRI;

    /**
     * A predicate that returns true if the subject of the provided quad is an IRI
     */
    public static final Predicate<Quad> hasSubjectIRI = quad -> quad.getSubject() instanceof IRI;

    /**
     * A predicate that returns whether the object of the quad is in the repository domain
     * @param domain the domain
     * @return a predicate that returns true if the object of the triple is in the repository domain
     */
    public static Predicate<Quad> inDomain(final String domain) {
        return hasObjectIRI.and(quad -> ((IRI) quad.getObject()).getIRIString().split("/", 2)[0].equals(domain));
    }

    /**
     * A predicate to determine if the object of the quad is equivalent to the provided IRI
     * @param identifier the identifier
     * @return a predicate that returns true if the object is equivalent to the same resource
     */
    public static Predicate<Quad> objectIsSameResource(final IRI identifier) {
        return hasObjectIRI.and(quad -> ((IRI) quad.getObject()).getIRIString().split("#", 2)[0]
                    .equals(identifier.getIRIString()));
    }


    /**
     * A predicate to determine if the subject of the quad is equivalent to the provided IRI
     * @param identifier the identifier
     * @return a predicate that returns true if the subject is equivalent to the same resource
     */
    public static Predicate<Quad> subjectIsSameResource(final IRI identifier) {
        return hasSubjectIRI.and(quad -> ((IRI) quad.getSubject()).getIRIString().split("#", 2)[0]
                    .equals(identifier.getIRIString()));
    }

    /**
     * Get the IRI of the parent resource, if it exists
     * @param identifier the identifier
     * @return the parent if it exists
     */
    public static Optional<String> getParent(final String identifier) {
        return of(identifier.lastIndexOf('/')).filter(idx -> idx > 0).map(idx -> identifier.substring(0, idx));
    }

    public static String serialize(final Dataset dataset) {
        if (nonNull(dataset)) {
            final DatasetGraph datasetGraph = create();
            final StringWriter str = new StringWriter();
            dataset.stream().map(quad -> rdf.asJenaQuad(quad)).forEach(datasetGraph::add);
            write(str, datasetGraph, NQUADS);
            return str.toString();
        }
        return "";
    }

    public static Dataset deserialize(final String data) {
        final DatasetGraph dataset = create();
        if (nonNull(data)) {
            read(dataset, new StringReader(data), null, NQUADS);
        }
        return rdf.asDataset(dataset);
    }

    private RDFUtils() {
        // prevent instantiation
    }
}
