package io.datanerds.grpc;

import io.datanerds.grpc.integrationtests.PingPongGrpc;
import io.datanerds.grpc.integrationtests.PingPongProto;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PingPongServer {
    private static final Logger logger = LoggerFactory.getLogger(PingPongServer.class);
    private Server server;
    private final int port;

    public PingPongServer() {
        this(55551);
    }

    public PingPongServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        this.server = ServerBuilder.forPort(this.port)
                .addService(new PingPongGrpc.PingPongImplBase() {
                    @Override
                    public void pingPong(PingPongProto.Ping request,
                            StreamObserver<PingPongProto.Pong> responseObserver) {
                        String pong = String.format("PONG-%d: %s", PingPongServer.this.port, request.getPing());
                        PingPongProto.Pong response = PingPongProto.Pong.newBuilder().setPong(pong).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        logger.info("Server started, listening on " + this.port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                PingPongServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() {
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (this.server != null) {
            this.server.awaitTermination();
        }
    }
}
