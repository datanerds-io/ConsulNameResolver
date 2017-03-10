package io.datanerds.grpc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public class ConsulNameResolverProvider extends NameResolverProvider {
    private static final Logger logger = LoggerFactory.getLogger(ConsulNameResolverProvider.class);

    private static final String SCHEME = "consul";
    private static final String QUERY_PARAMETER_TAGS = "tag";
    private static final String QUERY_PARAMETER_DATACENTER = "dc";

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    private void checkScheme(final URI uri) {
        if (!SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new RuntimeException("URI scheme is not 'consul'");
        }
    }

    protected ConsulQueryParameter sanitizeInput(final URI uri) {
        checkScheme(uri);
        Preconditions.checkNotNull(uri.getHost(), "Host cannot be empty");
        Preconditions.checkNotNull(uri.getPath(), "Path cannot be empty");
        Preconditions.checkArgument(uri.getPath().startsWith("/"),
                "the path component (%s) of the target (%s) must start with '/'", uri.getPath(), uri);

        Map<String, List<String>> queryParameters = splitQuery(uri);
        Preconditions.checkArgument(!(queryParameters.containsKey(QUERY_PARAMETER_DATACENTER)
                        && queryParameters.get(QUERY_PARAMETER_DATACENTER).size() != 1),
                "Only one datacenter can be defined");

        String consul;
        if (uri.getPort() == -1) {
            consul = uri.getHost() + ":" + ConsulNameResolver.DEFAULT_PORT;
        } else {
            consul = uri.getHost() + ":" + uri.getPort();
        }

        String service = uri.getPath().substring(1);

        String datacenter = null;
        if (queryParameters.containsKey(QUERY_PARAMETER_DATACENTER)) {
            datacenter = queryParameters.get(QUERY_PARAMETER_DATACENTER).get(0);
        }

        List<String> tags = null;
        if (queryParameters.containsKey(QUERY_PARAMETER_TAGS)) {
            tags = queryParameters.get(QUERY_PARAMETER_TAGS);
        }

        return new ConsulQueryParameter.Builder(service)
                .withConsulAddress(consul)
                .withDatacenter(datacenter)
                .withTags(tags)
                .build();
    }

    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        return new ConsulNameResolver(sanitizeInput(targetUri));
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    Map<String, List<String>> splitQuery(URI url) {
        if (Strings.isNullOrEmpty(url.getQuery())) {
            return Collections.emptyMap();
        }

        return Arrays.stream(url.getQuery().split("&"))
                .map(this::splitQueryParameter)
                .filter(x -> !Strings.isNullOrEmpty(x.getValue()))
                .collect(Collectors.groupingBy(AbstractMap.SimpleImmutableEntry::getKey,
                        LinkedHashMap::new,
                        mapping(Map.Entry::getValue, toList())
                ));
    }

    private AbstractMap.SimpleImmutableEntry<String, String> splitQueryParameter(String it) {
        int idx = it.indexOf("=");
        String key = idx > 0 ? it.substring(0, idx) : it;
        String value = idx > 0 && it.length() > idx + 1 ? it.substring(idx + 1) : null;
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }
}
