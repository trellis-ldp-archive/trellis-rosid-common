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

    public static final String TOPIC_DELETE = "trellis.delete";

    public static final String TOPIC_UPDATE = "trellis.update";

    public static final String TOPIC_RECACHE = "trellis.cache";

    public static final String TOPIC_LDP_CONTAINER_ADD = "trellis.ldpcontainer.add";

    public static final String TOPIC_LDP_CONTAINER_DELETE = "trellis.ldpcontainer.delete";

    private Constants() {
        // prevent instantiation
    }
}
