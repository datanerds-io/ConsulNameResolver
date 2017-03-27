package io.datanerds.grpc;

import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class URIUtilsTest {

    @Test
    public void splitQuery() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service");
        Map<String, List<String>> result = URIUtils.splitQuery(input);

        assertThat(result.keySet(), hasSize(0));

    }

    @Test
    public void splitQueryWithEmptyDCandTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=&tag=&tag=");
        Map<String, List<String>> result = URIUtils.splitQuery(input);

        assertThat(result.keySet(), hasSize(0));
    }

    @Test
    public void splitQueryWithDC() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1");
        Map<String, List<String>> result = URIUtils.splitQuery(input);

        assertThat(result.keySet(), hasSize(1));
        assertThat(result.get("dc"), hasSize(1));
    }

    @Test
    public void splitQueryWithJunkParams() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?foo=test&bar=baz");
        Map<String, List<String>> result = URIUtils.splitQuery(input);

        assertThat(result.keySet(), hasSize(0));
    }

    @Test
    public void splitQueryWithTag() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?tag=foo");
        Map<String, List<String>> result = URIUtils.splitQuery(input);

        assertThat(result.keySet(), hasSize(1));
        assertThat(result.get("tag"), hasSize(1));
    }

    @Test
    public void splitQueryWithTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?tag=foo&tag=bar&tag=baz");
        Map<String, List<String>> result = URIUtils.splitQuery(input);

        assertThat(result.keySet(), hasSize(1));
        assertThat(result.get("tag"), hasSize(3));
    }

    @Test
    public void splitQueryWithDCandTags() throws Exception {
        URI input = new URI("consul://localhost:1234/test_service?dc=dc1&tag=foo&tag=bar&tag=baz");
        Map<String, List<String>> result = URIUtils.splitQuery(input);

        assertThat(result.keySet(), hasSize(2));
        assertThat(result.get("dc"), hasSize(1));
        assertThat(result.get("tag"), hasSize(3));
    }

}