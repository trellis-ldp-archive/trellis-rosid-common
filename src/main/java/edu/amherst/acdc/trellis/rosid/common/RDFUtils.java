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

import static edu.amherst.acdc.trellis.vocabulary.PROV.endedAtTime;
import static edu.amherst.acdc.trellis.vocabulary.PROV.wasGeneratedBy;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.PreferAudit;
import static edu.amherst.acdc.trellis.vocabulary.XSD.dateTime;
import static java.util.Optional.of;

import java.time.Instant;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;

/**
 * @author acoburn
 */
public final class RDFUtils {

    private static final RDF rdf = ServiceLoader.load(RDF.class).iterator().next();

    /**
     * Get a singleton RDF instance
     * @return the RDF instance
     */
    public static RDF getInstance() {
        return rdf;
    }

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
            .map(term -> rdf.createQuad(PreferAudit, term, endedAtTime,
                    rdf.createLiteral(time.toString(), dateTime))).limit(1);
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
        if (identifier.endsWith("/")) {
            return getParent(identifier.substring(0, identifier.length() - 1));
        }
        return of(identifier.lastIndexOf('/')).filter(idx -> idx > 0).map(idx -> identifier.substring(0, idx));
    }

    private RDFUtils() {
        // prevent instantiation
    }
}
