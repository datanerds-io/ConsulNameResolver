package io.datanerds.grpc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ConsulNameResolverProviderTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void basicChecks() throws Exception {
        ConsulNameResolverProvider provider = new ConsulNameResolverProvider();
        assertTrue(provider.isAvailable());
        assertEquals(5, provider.priority());
        assertEquals("consul", provider.getDefaultScheme());
    }

    @Test
    public void newNameResolver() throws Exception {

    }

    @Test
    public void splitQuery() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service");
        Map<String, List<String>> result = new ConsulNameResolverProvider().splitQuery(input);

        assertEquals(0, result.keySet().size());
    }

    @Test
    public void splitQueryWithEmptyDCandTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=&tag=&tag=");
        Map<String, List<String>> result = new ConsulNameResolverProvider().splitQuery(input);

        assertEquals(0, result.keySet().size());
    }

    @Test
    public void splitQueryWithDC() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1");
        Map<String, List<String>> result = new ConsulNameResolverProvider().splitQuery(input);

        assertEquals(1, result.keySet().size());
        assertEquals(1, result.get("dc").size());
    }

    @Test
    public void splitQueryWithTag() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?tag=foo");
        Map<String, List<String>> result = new ConsulNameResolverProvider().splitQuery(input);

        assertEquals(1, result.keySet().size());
        assertEquals(1, result.get("tag").size());
    }

    @Test
    public void splitQueryWithTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?tag=foo&tag=bar&tag=baz");
        Map<String, List<String>> result = new ConsulNameResolverProvider().splitQuery(input);

        assertEquals(1, result.keySet().size());
        assertEquals(3, result.get("tag").size());
    }

    @Test
    public void splitQueryWithDCandTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1&tag=foo&tag=bar&tag=baz");
        Map<String, List<String>> result = new ConsulNameResolverProvider().splitQuery(input);

        assertEquals(2, result.keySet().size());
        assertEquals(1, result.get("dc").size());
        assertEquals(3, result.get("tag").size());
    }

    @Test
    public void testSanitizeInput() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service");
        ConsulQueryParameter params = new ConsulNameResolverProvider().sanitizeInput(input);

        assertFalse(params.datacenter.isPresent());
        assertFalse(params.tags.isPresent());
        assertTrue(params.consulAddress.isPresent());
        assertEquals("localhost:1234", params.consulAddress.get());
        assertEquals("test_service", params.service);
    }

    @Test
    public void testSanitizeInputWithDC() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1");
        ConsulQueryParameter params = new ConsulNameResolverProvider().sanitizeInput(input);

        assertTrue(params.datacenter.isPresent());
        assertEquals("dc1", params.datacenter.get());
        assertFalse(params.tags.isPresent());
        assertTrue(params.consulAddress.isPresent());
        assertEquals("localhost:1234", params.consulAddress.get());
        assertEquals("test_service", params.service);
    }

    @Test
    public void testSanitizeInputWithTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?tag=foo&tag=bar&tag=baz");
        ConsulQueryParameter params = new ConsulNameResolverProvider().sanitizeInput(input);

        assertFalse(params.datacenter.isPresent());
        assertTrue(params.tags.isPresent());
        assertTrue(params.tags.get().contains("foo"));
        assertTrue(params.tags.get().contains("bar"));
        assertTrue(params.tags.get().contains("baz"));
        assertTrue(params.consulAddress.isPresent());
        assertEquals("localhost:1234", params.consulAddress.get());
        assertEquals("test_service", params.service);
    }

    @Test
    public void testSanitizeInputWithDCandTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1&tag=foo&tag=bar&tag=baz");
        ConsulQueryParameter params = new ConsulNameResolverProvider().sanitizeInput(input);

        assertTrue(params.datacenter.isPresent());
        assertEquals("dc1", params.datacenter.get());
        assertTrue(params.tags.isPresent());
        assertTrue(params.tags.get().contains("foo"));
        assertTrue(params.tags.get().contains("bar"));
        assertTrue(params.tags.get().contains("baz"));
        assertTrue(params.consulAddress.isPresent());
        assertEquals("localhost:1234", params.consulAddress.get());
        assertEquals("test_service", params.service);
    }
}