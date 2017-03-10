package io.datanerds.grpc;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.ImmutableCatalogOptions;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConsulNameResolver extends NameResolver {
    public static final String DEFAULT_ADDRESS = "localhost";
    public static final Integer DEFAULT_PORT = 8383;
    private static final Logger logger = LoggerFactory.getLogger(ConsulNameResolver.class);
    private final String authority;
    private final String service;
    @GuardedBy("this")
    private Listener listener;
    private Consul consul;
    @GuardedBy("this")
    private HealthClient healthClient;
    private Optional<String> datacenter;
    private Optional<List<String>> tags;

    public ConsulNameResolver(ConsulQueryParameter parameter) {
        this.authority = parameter.consulAddress.orElse(DEFAULT_ADDRESS + ":" + DEFAULT_PORT); //todo move to URI
        this.service = parameter.service;
        this.datacenter = parameter.datacenter;
        this.tags = parameter.tags;

        HostAndPort consulHostAndPort = HostAndPort.fromString(this.authority).withDefaultPort(DEFAULT_PORT);
        this.consul = Consul.builder().withHostAndPort(consulHostAndPort).build();
        this.healthClient = this.consul.healthClient();
    }

    @Override
    public String getServiceAuthority() {
        return this.authority;
    }

    protected ImmutableCatalogOptions buildCatalogOptions() {
        ImmutableCatalogOptions.Builder options = ImmutableCatalogOptions.builder();

        if (this.datacenter.isPresent()) {
            options.datacenter(com.google.common.base.Optional.of(this.datacenter.get()));
        }

        if (this.tags.isPresent()) {
            this.tags.get().forEach(options::tag);
        }
        return options.build();
    }

    @Override
    public void start(Listener listener) {
        Preconditions.checkState(this.listener == null, "already started");
        this.listener = Preconditions.checkNotNull(listener, "listener");

        logger.debug(String.format("Resolving service '%s' using consul at '%s'", this.service, this.authority));

        ServiceHealthCache svHealth =
                ServiceHealthCache.newCache(this.healthClient, this.service, true, buildCatalogOptions(), 5);

        svHealth.addListener(newValues -> {
            List<ResolvedServerInfoGroup> servers = new ArrayList<>();

            if (newValues.isEmpty()) {
                logger.warn(String.format("No servers could be resolved for %s", this.service));
                listener.onError(Status.UNAVAILABLE.augmentDescription(String.format(
                        "No servers could be resolved for service '%s' from authority '%s'",
                        this.service, this.authority)));
                listener.onUpdate(servers, Attributes.EMPTY);
                return;
            }

            for (ServiceHealthKey key : newValues.keySet()) {
                InetSocketAddress socketAddress = new InetSocketAddress(key.getHost(), key.getPort());
                ResolvedServerInfoGroup.Builder builder = ResolvedServerInfoGroup.builder()
                        .add(new ResolvedServerInfo(socketAddress, Attributes.EMPTY));
                servers.add(builder.build());
            }
            listener.onUpdate(servers, Attributes.EMPTY);
        });

        try {
            svHealth.start();

            ConsulResponse<List<ServiceHealth>> healthyServiceInstances =
                    this.healthClient.getHealthyServiceInstances(this.service, buildCatalogOptions());
            if (healthyServiceInstances.getResponse().isEmpty()) {
                listener.onError(Status.UNAVAILABLE.augmentDescription(String.format(
                        "No servers could be resolved for service '%s' from authority '%s'",
                        this.service, this.authority)));
            }
        } catch (Exception ex) {
            throw new RuntimeException("Exception while trying to start consul client for name resolution", ex);
        }

    }

    @Override
    public void shutdown() {
        this.consul.destroy();
    }
}
