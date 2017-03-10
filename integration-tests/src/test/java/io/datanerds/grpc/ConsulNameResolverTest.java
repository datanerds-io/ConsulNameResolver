package io.datanerds.grpc;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.*;
import com.orbitz.consul.model.health.ServiceHealth;
import com.pszymczyk.consul.junit.ConsulResource;
import io.datanerds.grpc.ConsulQueryParameter.Builder;
import io.datanerds.grpc.integrationtests.PingPongGrpc;
import io.datanerds.grpc.integrationtests.PingPongProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ConsulNameResolverTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsulNameResolverTest.class);

    @ClassRule
    public static final ConsulResource consulResource = new ConsulResource();

    @Test
    public void checkIfConsulIsUpAndHealthy() throws Throwable {
        await().atMost(15, SECONDS).until(() -> {
            try {
                Consul consul = Consul.builder().withHostAndPort(HostAndPort.fromParts("localhost", consulResource
                        .getHttpPort())).build();
                HealthClient healthClient = consul.healthClient();
                List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances("consul").getResponse();
                return !nodes.isEmpty();
            } catch (ConsulException ex) {
                logger.debug("Can't connect to Consul", ex);
                return false;
            }
        });
    }

    @Test
    public void checkPingPongServer() throws Exception {
        int port = 55555;
        PingPongServer server = new PingPongServer(port);
        server.start();

        ManagedChannel channel = ManagedChannelBuilder
                .forTarget("localhost:" + port)
                .usePlaintext(true)
                .build();

        PingPongGrpc.PingPongBlockingStub blockingStub = PingPongGrpc.newBlockingStub(channel);
        PingPongProto.Ping ping = PingPongProto.Ping.newBuilder().setPing("PING").build();
        PingPongProto.Pong pong = blockingStub.pingPong(ping);
        assertEquals("PONG-" + port + ": " + ping.getPing(), pong.getPong());
        server.stop();
    }

    @Test
    public void simpleConsulLookup() throws Exception {
        consulResource.reset();
        int port = 55556;
        PingPongServer server = new PingPongServer(port);
        server.start();

        registerConsulService(consulResource.getHttpPort(), "test_v1", port, "test_v1");
        ManagedChannel channel = ManagedChannelBuilder
                .forTarget("consul://localhost:" + consulResource.getHttpPort() + "/test_v1")
                .nameResolverFactory(
                        new ConsulNameResolverProvider()
                )
                .usePlaintext(true)
                .build();

        PingPongGrpc.PingPongBlockingStub blockingStub = PingPongGrpc.newBlockingStub(channel);
        PingPongProto.Ping ping = PingPongProto.Ping.newBuilder().setPing("PING").build();
        PingPongProto.Pong pong = blockingStub.pingPong(ping);
        assertEquals("PONG-" + port + ": " + ping.getPing(), pong.getPong());
        server.stop();
    }

    @Test
    public void consulLookupWithTags() throws Exception {
        consulResource.reset();

        int port1 = 55555;
        int port2 = 55556;
        String serviceName = "test_v1";
        String serviceId = UUID.randomUUID().toString();

        String[] tags = {"foo", "bar", "baz"};

        ConsulQueryParameter params = new Builder(serviceName)
                .withConsulAddress("localhost:" + consulResource.getHttpPort())
                .withTag("foo")
                .withTag("bar")
                .withTag("baz")
                .build();

        registerConsulService(consulResource.getHttpPort(), serviceName, port1, serviceId, tags);

        MockListener listener = new MockListener();
        ConsulNameResolver resolver = new ConsulNameResolver(params);
        resolver.start(listener);
        await().atMost(5, SECONDS).until(listener.getServers()::size, is(1));

        registerConsulService(consulResource.getHttpPort(), serviceName, port2, UUID.randomUUID().toString(), tags);
        await().atMost(5, SECONDS).until(listener.getServers()::size, is(2));

        resolver.shutdown();
    }

    @Test
    public void consulLookupWithWrongTags() throws Exception {
        consulResource.reset();

        int port1 = 55555;
        int port2 = 55556;
        String serviceName = "test_v1";

        String[] tags = {"foo", "bar", "baz"};

        ConsulQueryParameter params = new Builder(serviceName)
                .withConsulAddress("localhost:" + consulResource.getHttpPort())
                .withTag("OTHER TAG")
                .withTag("ANOTHER TAG")
                .withTag("SOMETHING ELSE")
                .build();

        registerConsulService(consulResource.getHttpPort(), serviceName, port1, UUID.randomUUID().toString(), tags);

        MockListener listener = new MockListener();
        ConsulNameResolver resolver = new ConsulNameResolver(params);
        resolver.start(listener);

        await().atMost(5, SECONDS).until(listener.getErrors()::size, greaterThanOrEqualTo(1));
        resolver.shutdown();
    }

    @Test
    public void consulLookupWithPartiallyMatchingTags() throws Exception {
        consulResource.reset();

        int port1 = 55555;
        int port2 = 55556;
        String serviceName = "test_v1";

        String[] tags = {"foo", "bar", "baz"};

        ConsulQueryParameter params = new Builder(serviceName)
                .withConsulAddress("localhost:" + consulResource.getHttpPort())
                .withTag("foo")
                .build();

        registerConsulService(consulResource.getHttpPort(), serviceName, port1, UUID.randomUUID().toString(), tags);

        MockListener listener = new MockListener();
        ConsulNameResolver resolver = new ConsulNameResolver(params);
        resolver.start(listener);

        await().atMost(5, SECONDS).until(listener.getServers()::size, is(1));
        resolver.shutdown();
    }

    @Test
    public void multiConsulLookup() throws Exception {
        consulResource.reset();

        int port1 = 55555;
        int port2 = 55556;
        String serviceName = "test_v1";

        ConsulQueryParameter params = new Builder(serviceName)
                .withConsulAddress("localhost:" + consulResource.getHttpPort())
                .build();

        registerConsulService(consulResource.getHttpPort(), serviceName, port1, UUID.randomUUID().toString());

        MockListener listener = new MockListener();
        ConsulNameResolver resolver = new ConsulNameResolver(params);
        resolver.start(listener);
        await().atMost(5, SECONDS).until(listener.getServers()::size, is(1));

        registerConsulService(consulResource.getHttpPort(), serviceName, port2, UUID.randomUUID().toString());
        await().atMost(5, SECONDS).until(listener.getServers()::size, is(2));

        resolver.shutdown();
    }

    @Test
    public void removeNodesFromConsul() throws Exception {
        consulResource.reset();

        int port1 = 55555;
        int port2 = 55556;
        int port3 = 55557;

        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        String id3 = UUID.randomUUID().toString();

        String serviceName = "test_v1";

        ConsulQueryParameter params = new Builder(serviceName)
                .withConsulAddress("localhost:" + consulResource.getHttpPort())
                .build();

        String url = String.format("%s://%s:%d/%s?dc=dc1", "consul", "localhost", consulResource
                .getHttpPort(), serviceName);

        registerConsulService(consulResource.getHttpPort(), serviceName, port1, id1);
        registerConsulService(consulResource.getHttpPort(), serviceName, port2, id2);
        registerConsulService(consulResource.getHttpPort(), serviceName, port3, id3);

        MockListener listener = new MockListener();

        ConsulNameResolver resolver = new ConsulNameResolver(params);
        resolver.start(listener);

        assertEquals(0, listener.getErrors().size());

        await().atMost(5, SECONDS).until(listener.getServers()::size, is(3));

        unregisterConsulService(consulResource.getHttpPort(), id3);
        await().atMost(5, SECONDS).until(listener.getServers()::size, is(2));

        unregisterConsulService(consulResource.getHttpPort(), id2);
        await().atMost(5, SECONDS).until(listener.getServers()::size, is(1));

        unregisterConsulService(consulResource.getHttpPort(), id1);
        await().atMost(5, SECONDS).until(listener.getServers()::size, is(0));

        assertNotEquals(0, listener.getErrors().size());
        resolver.shutdown();
    }

    private void registerConsulService(int consulPort, String serviceName, int servicePort, String serviceId)
            throws NotRegisteredException {
        this.registerConsulService(consulPort, serviceName, servicePort, serviceId, new String[0]);
    }

    private void registerConsulService(int consulPort, String serviceName, int servicePort, String serviceId,
            String... tags)
            throws NotRegisteredException {
        Consul consul = Consul.builder().withHostAndPort(HostAndPort.fromParts("localhost", consulPort)).build();
        AgentClient agentClient = consul.agentClient();
        agentClient.register(servicePort, 60L, serviceName, serviceId, tags);
        agentClient.pass(serviceId);
        consul.destroy();
    }

    private boolean isConsulServiceRegistered(int consulPort, String serviceId) {
        Consul consul = Consul.builder().withHostAndPort(HostAndPort.fromParts("localhost", consulPort)).build();
        AgentClient agentClient = consul.agentClient();
        boolean isRegistered = agentClient.isRegistered(serviceId);
        consul.destroy();
        return isRegistered;
    }

    private void unregisterConsulService(int consulPort, String serviceId)
            throws NotRegisteredException {
        Consul consul = Consul.builder().withHostAndPort(HostAndPort.fromParts("localhost", consulPort)).build();
        AgentClient agentClient = consul.agentClient();
        agentClient.deregister(serviceId);
        consul.destroy();
    }

}
