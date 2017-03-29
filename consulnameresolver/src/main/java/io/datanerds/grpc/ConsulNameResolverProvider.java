package io.datanerds.grpc;

import com.google.common.base.Preconditions;
import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;

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

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        //Contract is to return null if resolver is not able to handle uri
        try {
            return new ConsulNameResolver(validateInputURI(targetUri));
        } catch (IllegalArgumentException | NullPointerException ex) {
            return null;
        }
    }

    private void validateScheme(URI uri) {
        if (!SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("scheme for uri is not 'consul'");
        }
    }

    protected ConsulQueryParameter validateInputURI(final URI uri) {
        validateScheme(uri);
        Preconditions.checkNotNull(uri.getHost(), "Host cannot be empty");
        Preconditions.checkNotNull(uri.getPath(), "Path cannot be empty");
        Preconditions.checkArgument(uri.getPath().startsWith("/"),
                "the path component (%s) of the target (%s) must start with '/'", uri.getPath(), uri);

        Map<String, List<String>> splitQuery = URIUtils.splitQuery(uri);
        Map<String, List<String>> queryParameters = URIUtils.splitQuery(uri);
        Preconditions.checkArgument(!(queryParameters.containsKey(QUERY_PARAMETER_DATACENTER)
                        && queryParameters.get(QUERY_PARAMETER_DATACENTER).size() != 1),
                "Only one datacenter can be defined");

        String consul;
        if (uri.getPort() == -1) {
            consul = String.format("%s:%s", uri.getHost(), ConsulNameResolver.DEFAULT_PORT);
        } else {
            consul = String.format("%s:%s", uri.getHost(), uri.getPort());
        }

        String service = uri.getPath().substring(1);

        ConsulQueryParameter.Builder builder = new ConsulQueryParameter.Builder(service)
                .withConsulAddress(consul);

        if (queryParameters.containsKey(QUERY_PARAMETER_DATACENTER)) {
            builder.withDatacenter(queryParameters.get(QUERY_PARAMETER_DATACENTER).get(0));
        }
        if (queryParameters.containsKey(QUERY_PARAMETER_TAGS)) {
            builder.withTags(queryParameters.get(QUERY_PARAMETER_TAGS));
        }

        return builder.build();
    }

}
