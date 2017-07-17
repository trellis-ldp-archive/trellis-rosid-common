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
import static java.util.Collections.emptyList;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.trellisldp.spi.RDFUtils.getInstance;
import static org.trellisldp.vocabulary.RDF.type;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;

import org.trellisldp.spi.Event;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.PROV;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
class Notification implements Event {

    private static final RDF rdf = getInstance();

    private final IRI identifier;
    private final IRI target;
    private final Dataset data;
    private final Instant created;

    public Notification(final String identifier, final Dataset data) {
        this.target = rdf.createIRI(identifier);
        this.data = data;
        this.identifier = rdf.createIRI("urn:uuid:" + randomUUID().toString());
        this.created = now();
    }

    public IRI getIdentifier() {
        return identifier;
    }

    public Collection<IRI> getAgents() {
        return data.getGraph(Trellis.PreferAudit)
            .map(graph -> graph.stream(null, PROV.wasAssociatedWith, null).map(Triple::getObject)
                    .filter(term -> term instanceof IRI).map(term -> (IRI) term).collect(toList()))
            .orElse(emptyList());
    }

    public Optional<IRI> getTarget() {
        return of(target);
    }

    public Collection<IRI> getTypes() {
        return data.getGraph(Trellis.PreferAudit)
            .map(graph -> graph.stream(null, type, null).map(Triple::getObject)
                    .filter(term -> term instanceof IRI).map(term -> (IRI) term).collect(toList()))
            .orElse(emptyList());
    }

    public Collection<IRI> getTargetTypes() {
        final Set<IRI> graphs = new HashSet<>();
        graphs.add(Trellis.PreferServerManaged);
        graphs.add(Trellis.PreferUserManaged);

        return data.stream().filter(quad -> quad.getGraphName().filter(graphs::contains).isPresent())
            .filter(quad -> quad.getPredicate().equals(type)).map(Quad::getObject)
            .filter(term -> term instanceof IRI).map(term -> (IRI) term).distinct().collect(toList());
    }

    public Instant getCreated() {
        return created;
    }

    public Optional<IRI> getInbox() {
        return data.getGraph(Trellis.PreferUserManaged)
            .flatMap(graph -> graph.stream(null, LDP.inbox, null).map(Triple::getObject)
                    .filter(term -> term instanceof IRI).map(term -> (IRI) term).findFirst());
    }
}
