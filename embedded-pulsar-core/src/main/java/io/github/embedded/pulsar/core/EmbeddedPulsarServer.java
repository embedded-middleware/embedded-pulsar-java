package io.github.embedded.pulsar.core;

import io.github.embedded.pulsar.core.module.PartitionedTopicInfo;
import io.github.embedded.pulsar.core.module.TenantInfo;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddedPulsarServer {

    private final Vertx vertx = Vertx.vertx();

    private final EmbeddedPulsarConfig config;

    @Getter
    private final int tcpPort;

    @Getter
    private final int httpPort;

    private final PulsarEngine pulsarEngine;

    private NetServer netServer;

    private HttpServer httpServer;

    public EmbeddedPulsarServer() throws Exception {
        this(new EmbeddedPulsarConfig());
    }

    public EmbeddedPulsarServer(EmbeddedPulsarConfig config) throws Exception {
        this.config = config;
        if (config.getTcpPort() == 0) {
            this.tcpPort = SocketUtil.getFreePort();
        } else {
            this.tcpPort = config.getTcpPort();
        }
        if (config.getHttpPort() == 0) {
            this.httpPort = SocketUtil.getFreePort();
        } else {
            this.httpPort = config.getHttpPort();
        }
        this.pulsarEngine = new PulsarEngine();
    }

    public void start() throws Exception {
        netServer = vertx.createNetServer();
        netServer.connectHandler(socket -> {
        });
        netServer.listen(tcpPort, res -> {
            if (res.succeeded()) {
                log.info("Embedded Pulsar TCP server started on port {}", tcpPort);
            } else {
                log.error("Failed to start Embedded Pulsar TCP server", res.cause());
            }
        });

        httpServer = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.put("/admin/v2/tenants/:tenant").handler(this::handleCreateTenant);
        router.delete("/admin/v2/tenants/:tenant").handler(this::handleDeleteTenant);
        router.get("/admin/v2/tenants").handler(this::handleGetTenants);
        router.put("/admin/v2/namespaces/:tenant/:namespace").handler(this::handleCreateNamespace);
        router.delete("/admin/v2/namespaces/:tenant/:namespace").handler(this::handleDeleteNamespace);
        router.get("/admin/v2/namespaces/:tenant").handler(this::handleGetTenantNamespaces);
        router.put("/admin/v2/persistent/:tenant/:namespace/:topic/partitions")
                .handler(this::handleCreatePartitionedTopic);
        router.get("/admin/v2/persistent/:tenant/:namespace/:topic/partitions")
                .handler(this::handleGetPartitionedTopicInfo);
        router.delete("/admin/v2/persistent/:tenant/:namespace/:topic/partitions")
                .handler(this::handleDeletePartitionedTopic);



        httpServer.requestHandler(router).listen(httpPort, res -> {
            if (res.succeeded()) {
                log.info("Embedded Pulsar HTTP server started on port {}", httpPort);
            } else {
                log.error("Failed to start Embedded Pulsar HTTP server", res.cause());
            }
        });
    }

    private void handleCreateTenant(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        TenantInfo tenantInfo = Json.decodeValue(context.body().asString(), TenantInfo.class);
        pulsarEngine.createTenant(tenant, tenantInfo);
        context.response().setStatusCode(204).end("Tenant created");
    }

    private void handleDeleteTenant(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        pulsarEngine.deleteTenant(tenant);
        context.response().setStatusCode(204).end();
    }

    private void handleGetTenants(RoutingContext context) {
        context.response().setStatusCode(200).end(Json.encode(pulsarEngine.getTenants()));
    }

    private void handleGetTenantNamespaces(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        context.response().setStatusCode(200).end(Json.encode(pulsarEngine.getTenantNamespaces(tenant)));
    }

    private void handleCreateNamespace(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        String namespace = context.pathParam("namespace");
        pulsarEngine.createNamespace(tenant, namespace);
        context.response().setStatusCode(204).end("Namespace created");
    }

    private void handleDeleteNamespace(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        String namespace = context.pathParam("namespace");
        pulsarEngine.deleteNamespace(tenant, namespace);
        context.response().setStatusCode(204).end();
    }

    private void handleCreatePartitionedTopic(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        String namespace = context.pathParam("namespace");
        String topic = context.pathParam("topic");
        int numPartitions = Integer.parseInt(context.body().asString());
        pulsarEngine.createPartitionedTopic(tenant, namespace, topic, numPartitions);
        context.response().setStatusCode(204).end("Partitioned topic created");
    }

    private void handleDeletePartitionedTopic(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        String namespace = context.pathParam("namespace");
        String topic = context.pathParam("topic");
        pulsarEngine.deletePartitionedTopic(tenant, namespace, topic);
        context.response().setStatusCode(204).end();
    }

    private void handleGetPartitionedTopicInfo(RoutingContext context) {
        String tenant = context.pathParam("tenant");
        String namespace = context.pathParam("namespace");
        String topic = context.pathParam("topic");
        PartitionedTopicInfo info = pulsarEngine.getPartitionedTopicInfo(tenant, namespace, topic);
        if (info != null) {
            context.response().setStatusCode(200).end(Json.encode(info));
        } else {
            context.response().setStatusCode(404).end("Partitioned topic not found");
        }
    }


    public void close() throws Exception {
        if (netServer != null) {
            netServer.close();
        }
        if (httpServer != null) {
            httpServer.close();
        }
    }
}
