package io.datanerds.grpc;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.ResolvedServerInfoGroup;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockListener implements NameResolver.Listener {
    private List<ResolvedServerInfoGroup> servers = Collections.synchronizedList(new ArrayList<>());
    private Attributes attributes;
    private List<Status> errors = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void onUpdate(List<ResolvedServerInfoGroup> servers, Attributes attributes) {
        this.servers.clear();
        this.servers.addAll(servers);
        this.attributes = attributes;
    }

    @Override
    public void onError(Status error) {
        this.errors.add(error);
    }

    public List<ResolvedServerInfoGroup> getServers() {
        return this.servers;
    }

    public Attributes getAttributes() {
        return this.attributes;
    }

    public List<Status> getErrors() {
        return this.errors;
    }
}
