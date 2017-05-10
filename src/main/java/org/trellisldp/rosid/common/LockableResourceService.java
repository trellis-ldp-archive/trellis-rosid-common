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

import static java.util.Objects.isNull;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.apache.curator.framework.imps.CuratorFrameworkState.LATENT;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.common.RDFUtils.getInstance;

import java.util.Properties;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.trellisldp.spi.ResourceService;
import org.trellisldp.spi.RuntimeRepositoryException;

/**
 * @author acoburn
 */
abstract class LockableResourceService implements ResourceService, AutoCloseable {

    private static final Logger LOGGER = getLogger(LockableResourceService.class);

    protected final Producer<String, Dataset> producer;

    protected final CuratorFramework curator;

    protected final RDF rdf = getInstance();

    private final String SESSION_ZNODE = "/session";

    protected LockableResourceService(final Properties kafkaProperties, final Properties zkProperties) {
        this(new KafkaProducer<>(addDefaults(kafkaProperties)),
                newClient(zkProperties.getProperty("connectString"),
                    new BoundedExponentialBackoffRetry(
                        Integer.parseInt(zkProperties.getProperty("retry.ms", "2000")),
                        Integer.parseInt(zkProperties.getProperty("retry.max.ms", "30000")),
                        Integer.parseInt(zkProperties.getProperty("retry.max", "10")))));
    }

    protected LockableResourceService(final Producer<String, Dataset> producer, final CuratorFramework curator) {
        this.producer = producer;
        this.curator = curator;
        if (LATENT.equals(curator.getState())) {
            this.curator.start();
        }
        try {
            this.curator.createContainers(SESSION_ZNODE);
        } catch (final Exception ex) {
            LOGGER.error("Could not create zk session node: {}", ex.getMessage());
            throw new RuntimeRepositoryException(ex);
        }
    }

    protected InterProcessLock getLock(final IRI identifier) {
        final String path = SESSION_ZNODE + "/" + md5Hex(identifier.getIRIString());
        return new InterProcessSemaphoreMutex(curator, path);
    }

    @Override
    public void close() {
        producer.close();
        curator.close();
    }

    private static Properties addDefaults(final Properties props) {
        if (isNull(props.getProperty("acks"))) {
            props.setProperty("acks", "all");
        }
        if (isNull(props.getProperty("retries"))) {
            props.setProperty("retries", "0");
        }
        if (isNull(props.getProperty("batch.size"))) {
            props.put("batch.size", "16384");
        }
        if (isNull(props.getProperty("linger.ms"))) {
            props.put("linger.ms", "1");
        }
        if (isNull(props.getProperty("buffer.memory"))) {
            props.put("buffer.memory", "33554432");
        }
        if (isNull(props.getProperty("key.serializer"))) {
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }
        if (isNull(props.getProperty("value.serializer"))) {
            props.put("value.serializer", "org.trellisldp.rosid.common.DatasetSerialization");
        }
        return props;
    }
}
