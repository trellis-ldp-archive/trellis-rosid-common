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

/**
 * @author acoburn
 */
public final class Constants {

    /**
     * The topic that notifies about resource deletion
     */
    public static final String TOPIC_DELETE = "trellis.delete";

    /**
     * The topic that notifies about object changes
     */
    public static final String TOPIC_EVENT = "trellis.event";

    /**
     * The topic that notifies about inbound reference additions
     */
    public static final String TOPIC_INBOUND_ADD = "trellis.inbound.add";

    /**
     * The topic that notifies about inbound reference deletions
     */
    public static final String TOPIC_INBOUND_DELETE = "trellis.inbound.delete";

    /**
     * The topic that is used to add LDP container triples
     */
    public static final String TOPIC_LDP_CONTAINER_ADD = "trellis.ldpcontainer.add";

    /**
     * The topic that is used to remove LDP container triples
     */
    public static final String TOPIC_LDP_CONTAINER_DELETE = "trellis.ldpcontainer.delete";

    /**
     * The aggregated (windowed) topic that notifies the cache to be regenerated
     */
    public static final String TOPIC_RECACHE = "trellis.cache";

    /**
     * The topic that notifies about resource modification
     */
    public static final String TOPIC_UPDATE = "trellis.update";

    private Constants() {
        // prevent instantiation
    }
}
