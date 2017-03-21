package io.datanerds.grpc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ConsulNameResolverProviderTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void basicChecks() throws Exception {
        ConsulNameResolverProvider provider = new ConsulNameResolverProvider();
        assertTrue(provider.isAvailable());
        assertThat(5, is(equalTo(provider.priority())));
        assertThat("consul", is(equalTo(provider.getDefaultScheme())));
    }

    @Test
    public void testSanitizeInputBadScheme() throws Exception {
        URI input = new URI("test://localhost:1234/test_service");
        this.thrown.expect(IllegalArgumentException.class);
        new ConsulNameResolverProvider().validateInputURI(input);
    }

    @Test
    public void testSanitizeInputBadHost() throws Exception {
        URI input = new URI("test:///test_service");
        this.thrown.expect(IllegalArgumentException.class);
        new ConsulNameResolverProvider().validateInputURI(input);
    }

    @Test
    public void testSanitizeInputBadPort() throws Exception {
        URI input = new URI("test://localhost:100000/test_service");
        this.thrown.expect(IllegalArgumentException.class);
        new ConsulNameResolverProvider().validateInputURI(input);
    }

    @Test
    public void testSanitizeInputBadService() throws Exception {
        URI input = new URI("test://localhost:1234/");
        this.thrown.expect(IllegalArgumentException.class);
        new ConsulNameResolverProvider().validateInputURI(input);
    }

    @Test
    public void testSanitizeInput() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service");
        ConsulQueryParameter params = new ConsulNameResolverProvider().validateInputURI(input);

        assertFalse(params.datacenter.isPresent());
        assertFalse(params.tags.isPresent());
        assertTrue(params.consulAddress.isPresent());
        assertThat("localhost:1234", is(equalTo(params.consulAddress.get())));
        assertThat("test_service", is(equalTo(params.service)));
    }

    @Test
    public void testSanitizeInputWithDC() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1");
        ConsulQueryParameter params = new ConsulNameResolverProvider().validateInputURI(input);

        assertTrue(params.datacenter.isPresent());
        assertThat("dc1", is(equalTo(params.datacenter.get())));
        assertFalse(params.tags.isPresent());
        assertTrue(params.consulAddress.isPresent());
        assertThat("localhost:1234", is(equalTo(params.consulAddress.get())));
        assertThat("test_service", is(equalTo(params.service)));
    }

    @Test
    public void testSanitizeInputWithTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?tag=foo&tag=bar&tag=baz");
        ConsulQueryParameter params = new ConsulNameResolverProvider().validateInputURI(input);

        assertFalse(params.datacenter.isPresent());
        assertTrue(params.tags.isPresent());
        assertThat(params.tags.get(), containsInAnyOrder("foo", "bar", "baz"));
        assertTrue(params.consulAddress.isPresent());
        assertThat("localhost:1234", is(equalTo(params.consulAddress.get())));
        assertThat("test_service", is(equalTo(params.service)));
    }

    @Test
    public void testSanitizeInputWithDCandTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1&tag=foo&tag=bar&tag=baz");
        ConsulQueryParameter params = new ConsulNameResolverProvider().validateInputURI(input);

        assertTrue(params.datacenter.isPresent());
        assertThat("dc1", is(equalTo(params.datacenter.get())));
        assertTrue(params.tags.isPresent());
        assertThat(params.tags.get(), containsInAnyOrder("foo", "bar", "baz"));
        assertTrue(params.consulAddress.isPresent());
        assertThat("localhost:1234", is(equalTo(params.consulAddress.get())));
        assertThat("test_service", is(equalTo(params.service)));
    }
}