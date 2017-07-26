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

import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_COORDINATION;
import static org.trellisldp.spi.RDFUtils.getInstance;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.trellisldp.spi.ResourceService;
import org.trellisldp.spi.RuntimeRepositoryException;

/**
 * @author acoburn
 */
abstract class LockableResourceService implements ResourceService {

    private static final Logger LOGGER = getLogger(LockableResourceService.class);

    protected final Producer<String, String> producer;

    protected final CuratorFramework curator;

    protected final RDF rdf = getInstance();

    protected LockableResourceService(final Producer<String, String> producer, final CuratorFramework curator) {
        this.producer = producer;
        this.curator = curator;
        try {
            this.curator.createContainers(ZNODE_COORDINATION);
        } catch (final Exception ex) {
            LOGGER.error("Could not create zk session node: {}", ex.getMessage());
            throw new RuntimeRepositoryException(ex);
        }
    }

    protected InterProcessLock getLock(final IRI identifier) {
        final String path = ZNODE_COORDINATION + "/" + md5Hex(identifier.getIRIString());
        return new InterProcessSemaphoreMutex(curator, path);
    }
}
