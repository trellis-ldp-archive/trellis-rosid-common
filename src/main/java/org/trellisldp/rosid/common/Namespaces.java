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

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_NAMESPACES;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.trellisldp.spi.NamespaceService;
import org.trellisldp.spi.RuntimeRepositoryException;

/**
 * @author acoburn
 */
public class Namespaces implements NamespaceService {

    private static final Logger LOGGER = getLogger(Namespaces.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final NodeCache cache;

    private Map<String, String> data = new HashMap<>();

    /**
     * Create a zookeeper-based namespace service
     * @param cache the nodecache
     */
    public Namespaces(final NodeCache cache) {
        this(cache, null);
    }

    /**
     * Create a zookeeper-based namespace service
     * @param cache the nodecache
     * @param filePath the file
     */
    public Namespaces(final NodeCache cache, final String filePath) {
        requireNonNull(cache, "NodeCache may not be null!");
        this.cache = cache;
        try {
            this.cache.getListenable().addListener(() -> data = read(cache.getCurrentData().getData()));
            this.data = init(filePath);
        } catch (final Exception ex) {
            LOGGER.error("Could not create a zk node cache: {}", ex);
            throw new RuntimeRepositoryException(ex);
        }
    }

    @Override
    public Map<String, String> getNamespaces() {
        return unmodifiableMap(data);
    }

    @Override
    public Optional<String> getNamespace(final String prefix) {
        return ofNullable(data.get(prefix));
    }

    @Override
    public Optional<String> getPrefix(final String namespace) {
        return data.entrySet().stream().filter(e -> e.getValue().equals(namespace)).map(Map.Entry::getKey).findFirst();
    }

    @Override
    public Boolean setPrefix(final String prefix, final String namespace) {
        if (data.containsKey(prefix)) {
            return false;
        }

        data.put(prefix, namespace);
        try {
            cache.getClient().create().orSetData().forPath(ZNODE_NAMESPACES, write(data));
            return true;
        } catch (final Exception ex) {
            LOGGER.error("Unable to set data: {}", ex.getMessage());
        }
        return false;
    }

    private static byte[] write(final Map<String, String> data) {
       try {
           return MAPPER.writeValueAsBytes(data);
       } catch (final IOException ex) {
            LOGGER.error("Unable to serialize data: {}", ex.getMessage());
       }
       return new byte[0];
    }

    private Map<String, String> init(final String filePath) throws Exception {
        if (nonNull(filePath)) {
            try (final FileInputStream input = new FileInputStream(new File(filePath))) {
                // TODO - JDK9 InputStream::readAllBytes
                final byte[] bytes = IOUtils.toByteArray(input);
                final Map<String, String> ns = read(bytes);
                cache.getClient().create().orSetData().forPath(ZNODE_NAMESPACES, bytes);
                return ns;
            } catch (final IOException ex) {
                LOGGER.warn("Unable to read provided file: {}", ex.getMessage());
            }
        }
        try {
            return read(cache.getClient().getData().forPath(ZNODE_NAMESPACES));
        } catch (final NoNodeException ex) {
            LOGGER.warn("No namespace data stored in ZooKeeper: {}", ex.getMessage());
            return new HashMap<>();
        }
    }

    private static Map<String, String> read(final byte[] data) {
        final Map<String, String> namespaces = new HashMap<>();
        try {
            of(MAPPER.readTree(data)).filter(JsonNode::isObject).ifPresent(json ->
                json.fields().forEachRemaining(node -> {
                    if (node.getValue().isTextual()) {
                        namespaces.put(node.getKey(), node.getValue().textValue());
                    } else {
                        LOGGER.warn("Ignoring non-textual node at key: {}", node.getKey());
                    }
                }));
        } catch (final IOException ex) {
            LOGGER.error("Could not read namespace data: {}", ex.getMessage());
        }
        return namespaces;
    }
}
