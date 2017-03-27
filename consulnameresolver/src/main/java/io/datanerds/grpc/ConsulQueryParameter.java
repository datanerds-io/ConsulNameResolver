package io.datanerds.grpc;

import java.util.*;

public class ConsulQueryParameter {
    public final Optional<String> consulAddress;
    public final String service;
    public final Optional<String> datacenter;
    public final Optional<List<String>> tags;

    private ConsulQueryParameter(Optional<String> consulAddress, String service,
            Optional<String> datacenter, Optional<List<String>> tags) {
        this.consulAddress = consulAddress;
        this.service = service;
        this.datacenter = datacenter;
        this.tags = tags;
    }

    public static class Builder {
        private String consulAddress;
        private String service;
        private String datacenter;
        private Set<String> tags;

        public Builder(String service) {
            if (service == null) {
                throw new RuntimeException("Service cannot be null");
            }
            this.service = service;
            this.tags = new HashSet<>();
        }

        public Builder withConsulAddress(String consulAddress) {
            this.consulAddress = consulAddress;
            return this;
        }

        public Builder withDatacenter(String datacenter) {
            this.datacenter = datacenter;
            return this;
        }

        public Builder withTag(String tag) {
            this.tags.add(tag);
            return this;
        }

        public Builder withTags(List<String> tags) {
            if (tags != null) {
                this.tags.addAll(tags);
            }
            return this;
        }

        public ConsulQueryParameter build() {
            Optional<String> address = Optional.ofNullable(this.consulAddress);
            Optional<String> datacenter = Optional.ofNullable(this.datacenter);
            Optional<List<String>> tags;
            if (this.tags.isEmpty()) {
                tags = Optional.empty();
            } else {
                tags = Optional.of(new ArrayList<>(this.tags));
            }
            return new ConsulQueryParameter(address, this.service, datacenter, tags);
        }
    }
}