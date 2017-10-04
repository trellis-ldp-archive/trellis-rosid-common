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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_NAMESPACES;

import java.io.File;
import java.net.URL;
import java.math.BigInteger;
import java.security.SecureRandom;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.trellisldp.api.NamespaceService;
import org.trellisldp.api.RuntimeRepositoryException;
import org.trellisldp.vocabulary.JSONLD;
import org.trellisldp.vocabulary.LDP;

/**
 * @author acoburn
 */
@RunWith(MockitoJUnitRunner.class)
public class NamespacesTest {

    private static final String nsDoc = "/testNamespaces.json";
    private static TestingServer curator;

    @Mock
    private static CuratorFramework mockCurator;

    @Mock
    private NodeCache mockCache;

    @Mock
    private ListenerContainer<NodeCacheListener> mockListenable;

    @Mock
    private GetDataBuilder mockDataBuilder;

    @BeforeClass
    public static void setUp() throws Exception {
        curator = new TestingServer(true);
    }

    @Before
    public void setUpMocks() throws Exception {
        when(mockCache.getClient()).thenReturn(mockCurator);
        when(mockCache.getListenable()).thenReturn(mockListenable);
    }

    @Test
    public void testReadNamespaces() throws Exception {
        final URL res = Namespaces.class.getResource(nsDoc);
        final NamespaceService service = new Namespaces(getZkCache(curator.getConnectString()), res.getPath());

        assertEquals(2, service.getNamespaces().size());
        assertEquals(LDP.URI, service.getNamespace("ldp").get());
        assertEquals("ldp", service.getPrefix(LDP.URI).get());
    }

    @Test
    public void testReadNamespacesNoFile() throws Exception {
        final URL res = Namespaces.class.getResource(nsDoc);
        final NamespaceService service = new Namespaces(getZkCache(curator.getConnectString()),
                res.getPath() + "Im-gonna-go-to-London.txt");

        assertEquals(0, service.getNamespaces().size());
    }

    @Test
    public void testWriteNamespaces() throws Exception {
        final File file = new File(Namespaces.class.getResource(nsDoc).getPath());
        final String filename = file.getParent() + "/" + randomFilename();
        final Namespaces svc1 = new Namespaces(getZkCache(curator.getConnectString()), file.getPath());
        assertEquals(2, svc1.getNamespaces().size());
        assertFalse(svc1.getNamespace("jsonld").isPresent());
        assertFalse(svc1.getPrefix(JSONLD.URI).isPresent());
        assertTrue(svc1.setPrefix("jsonld", JSONLD.URI));
        assertEquals(3, svc1.getNamespaces().size());
        assertEquals(JSONLD.URI, svc1.getNamespace("jsonld").get());
        assertEquals("jsonld", svc1.getPrefix(JSONLD.URI).get());

        final Namespaces svc2 = new Namespaces(getZkCache(curator.getConnectString()));
        assertEquals(3, svc2.getNamespaces().size());
        assertEquals(JSONLD.URI, svc2.getNamespace("jsonld").get());
        assertFalse(svc2.setPrefix("jsonld", JSONLD.URI));
    }

    @Test(expected = RuntimeRepositoryException.class)
    public void testErrorHandler() throws Exception {
        when(mockCache.getClient()).thenReturn(mockCurator);
        when(mockCurator.getData()).thenReturn(mockDataBuilder);
        doThrow(Exception.class).when(mockDataBuilder).forPath(ZNODE_NAMESPACES);
        new Namespaces(mockCache);
    }

    @Test
    public void testError2() throws Exception {
        when(mockCurator.getData()).thenReturn(mockDataBuilder);
        when(mockDataBuilder.forPath(ZNODE_NAMESPACES)).thenReturn("{}".getBytes(UTF_8));
        final Namespaces ns = new Namespaces(mockCache);
        assertFalse(ns.setPrefix("foo", "bar"));
    }

    private static NodeCache getZkCache(final String connectString) throws Exception {
        final CuratorFramework zk = newClient(connectString, new RetryNTimes(10, 1000));
        zk.start();
        final NodeCache cache = new NodeCache(zk, ZNODE_NAMESPACES);
        cache.start();
        return cache;
    }

    private static String randomFilename() {
        final SecureRandom random = new SecureRandom();
        final String filename = new BigInteger(50, random).toString(32);
        return filename + ".json";
    }

}
