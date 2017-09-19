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

import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.math.BigInteger;
import java.security.SecureRandom;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trellisldp.spi.NamespaceService;
import org.trellisldp.vocabulary.JSONLD;
import org.trellisldp.vocabulary.LDP;

/**
 * @author acoburn
 */
public class NamespacesTest {

    private static final String nsDoc = "/testNamespaces.json";
    private static TestingServer curator;

    @Test
    public void testReadNamespaces() throws Exception {
        final URL res = Namespaces.class.getResource(nsDoc);
        final NamespaceService service = new Namespaces(getZkClient(curator.getConnectString()), res.getPath());

        assertEquals(2, service.getNamespaces().size());
        assertEquals(LDP.URI, service.getNamespace("ldp").get());
        assertEquals("ldp", service.getPrefix(LDP.URI).get());
    }

    @Test
    public void testReadNamespacesNoFile() throws Exception {
        final URL res = Namespaces.class.getResource(nsDoc);
        final NamespaceService service = new Namespaces(getZkClient(curator.getConnectString()),
                res.getPath() + "Im-gonna-go-to-London.txt");

        assertEquals(0, service.getNamespaces().size());
    }

    @Test
    public void testWriteNamespaces() throws Exception {
        final File file = new File(Namespaces.class.getResource(nsDoc).getPath());
        final String filename = file.getParent() + "/" + randomFilename();
        try (final Namespaces svc1 = new Namespaces(getZkClient(curator.getConnectString()), file.getPath())) {
            assertEquals(2, svc1.getNamespaces().size());
            assertFalse(svc1.getNamespace("jsonld").isPresent());
            assertFalse(svc1.getPrefix(JSONLD.URI).isPresent());
            assertTrue(svc1.setPrefix("jsonld", JSONLD.URI));
            assertEquals(3, svc1.getNamespaces().size());
            assertEquals(JSONLD.URI, svc1.getNamespace("jsonld").get());
            assertEquals("jsonld", svc1.getPrefix(JSONLD.URI).get());
        }

        try (final Namespaces svc2 = new Namespaces(getZkClient(curator.getConnectString()))) {
            assertEquals(3, svc2.getNamespaces().size());
            assertEquals(JSONLD.URI, svc2.getNamespace("jsonld").get());
            assertFalse(svc2.setPrefix("jsonld", JSONLD.URI));
        }
    }

    private static CuratorFramework getZkClient(final String connectString) {
        final CuratorFramework zk = newClient(connectString, new RetryNTimes(10, 1000));
        zk.start();
        return zk;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        curator = new TestingServer(true);
    }

    private static String randomFilename() {
        final SecureRandom random = new SecureRandom();
        final String filename = new BigInteger(50, random).toString(32);
        return filename + ".json";
    }

}
