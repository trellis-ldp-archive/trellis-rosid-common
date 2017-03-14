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

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;

/**
 * @author acoburn
 */
public class Message {

    private final IRI identifier;

    private final Dataset dataset;

    /**
     * Create a Message object
     * @param identifier the identifier
     * @param dataset the dataset
     */
    public Message(final IRI identifier, final Dataset dataset) {
        this.identifier = identifier;
        this.dataset = dataset;
    }

    /**
     * Get the identifier
     * @return the identifier
     */
    public IRI getIdentifier() {
        return identifier;
    }

    /**
     * Get the dataset
     * @return the dataset
     */
    public Dataset getDataset() {
        return dataset;
    }
}
