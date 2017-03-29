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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConsulNameResolver extends NameResolver {
    public static final String DEFAULT_ADDRESS = "localhost";
    public static final Integer DEFAULT_PORT = 8383;
    private static final Logger logger = LoggerFactory.getLogger(ConsulNameResolver.class);
    private final String authority;
    private final String service;
    private Listener listener;
    private Consul consul;
    private HealthClient healthClient;
    private Optional<String> datacenter;
    private Optional<List<String>> tags;

    public ConsulNameResolver(ConsulQueryParameter parameter) {
        this.authority = parameter.consulAddress.orElse(
                String.format("%s:%s", DEFAULT_ADDRESS, DEFAULT_PORT)); //todo move to URI
        this.service = parameter.service;
        this.datacenter = parameter.datacenter;
        this.tags = parameter.tags;

        HostAndPort consulHostAndPort = HostAndPort.fromString(this.authority);
        this.consul = Consul.builder().withHostAndPort(consulHostAndPort).build();
        this.healthClient = this.consul.healthClient();
    }

    @Override
    public String getServiceAuthority() {
        return this.authority;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void start(Listener listener) {
        Preconditions.checkState(this.listener == null, "already started");
        this.listener = Preconditions.checkNotNull(listener, "listener");
        logger.debug("Resolving service '{}' using consul at '%{}'", this.service, this.authority);

        ServiceHealthCache svHealth =
                ServiceHealthCache.newCache(this.healthClient, this.service, true, buildCatalogOptions(), 5);

        svHealth.addListener((Map<ServiceHealthKey, ServiceHealth> newValues) -> {
            if (newValues.isEmpty()) {
                updateListenerWithEmptyResult();
                return;
            }
            listener.onUpdate(generateResolvedServerList(newValues), Attributes.EMPTY);
        });

        try {
            svHealth.start();
            runInitialResolve();
        } catch (Exception ex) {
            throw new RuntimeException("Exception while trying to start consul client for name resolution", ex);
        }
    }

    private ImmutableCatalogOptions buildCatalogOptions() {
        ImmutableCatalogOptions.Builder options = ImmutableCatalogOptions.builder();

        if (this.datacenter.isPresent()) {
            options.datacenter(com.google.common.base.Optional.of(this.datacenter.get()));
        }

        if (this.tags.isPresent()) {
            this.tags.get().forEach(options::tag);
        }
        return options.build();
    }

    private void updateListenerWithEmptyResult() {
        logger.warn("No servers could be resolved for '{}'", ConsulNameResolver.this.service);
        this.listener.onError(Status.UNAVAILABLE.augmentDescription(String.format(
                "No servers could be resolved for service '%s' from authority '%s'",
                ConsulNameResolver.this.service, ConsulNameResolver.this.authority)));
        this.listener.onUpdate(new ArrayList<>(), Attributes.EMPTY);
    }

    private List<ResolvedServerInfoGroup> generateResolvedServerList(Map<ServiceHealthKey, ServiceHealth> newValues) {
        return newValues
                .keySet()
                .stream()
                .map(key -> new InetSocketAddress(key.getHost(), key.getPort()))
                .map(socketAddress -> ResolvedServerInfoGroup.builder()
                        .add(new ResolvedServerInfo(socketAddress, Attributes.EMPTY)))
                .map(ResolvedServerInfoGroup.Builder::build).collect(Collectors.toList());
    }

    private void runInitialResolve() {
        ConsulResponse<List<ServiceHealth>> healthyServiceInstances =
                this.healthClient.getHealthyServiceInstances(this.service, buildCatalogOptions());
        if (healthyServiceInstances.getResponse().isEmpty()) {
            this.listener.onError(Status.UNAVAILABLE.augmentDescription(String.format(
                    "No servers could be resolved for service '%s' from authority '%s'",
                    this.service, this.authority)));
        }
    }

}
