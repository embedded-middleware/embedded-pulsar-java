package io.github.embedded.pulsar.core;

import io.github.embedded.pulsar.core.module.PartitionedTopicInfo;
import io.github.embedded.pulsar.core.module.TenantInfo;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetServer;
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
        httpServer.requestHandler(req -> {
            String[] pathSegments = req.path().substring(1).split("/");

            if (pathSegments.length > 0 && "tenants".equals(pathSegments[0])) {
                if (pathSegments.length == 1) {
                    if (req.method() == HttpMethod.GET) {
                        req.response().setStatusCode(200).end(Json.encode(pulsarEngine.getTenants()));
                    } else {
                        req.response().setStatusCode(405).end("Method Not Allowed");
                    }
                }
                else if (pathSegments.length == 2) {
                    String tenant = pathSegments[1];
                    HttpMethod method = req.method();
                    if (method.equals(HttpMethod.POST)) {
                        req.bodyHandler(buffer -> {
                            TenantInfo tenantInfo = Json.decodeValue(buffer, TenantInfo.class);
                            pulsarEngine.createTenant(tenant, tenantInfo);
                            req.response().setStatusCode(201).end("Tenant created");
                        });
                    } else if (method.equals(HttpMethod.DELETE)) {
                        pulsarEngine.deleteTenant(tenant);
                        req.response().setStatusCode(204).end();
                    } else if (method.equals(HttpMethod.GET)) {
                        TenantInfo tenantInfo = pulsarEngine.getTenantInfo(tenant);
                        if (tenantInfo != null) {
                            req.response().setStatusCode(200).end(Json.encode(tenantInfo));
                        } else {
                            req.response().setStatusCode(404).end("Tenant not found");
                        }
                    } else {
                        req.response().setStatusCode(405).end("Method Not Allowed");
                    }
                } else {
                    req.response().setStatusCode(404).end("Not Found");
                }
            } else if ("namespaces".equals(pathSegments[0])) {
                if (pathSegments.length == 2) {
                    String tenant = pathSegments[1];
                    if (req.method() == HttpMethod.GET) {
                        req.response().setStatusCode(200).end(Json.encode(pulsarEngine.getTenantNamespaces(tenant)));
                    } else {
                        req.response().setStatusCode(405).end("Method Not Allowed");
                    }
                } else if (pathSegments.length == 3) {
                    String tenant = pathSegments[1];
                    String namespace = pathSegments[2];
                    HttpMethod method = req.method();
                    if (method.equals(HttpMethod.POST)) {
                        pulsarEngine.createNamespace(tenant, namespace);
                        req.response().setStatusCode(201).end("Namespace created");
                    } else if (method.equals(HttpMethod.DELETE)) {
                        pulsarEngine.deleteNamespace(tenant, namespace);
                        req.response().setStatusCode(204).end();
                    } else {
                        req.response().setStatusCode(405).end("Method Not Allowed");
                    }
                } else {
                    req.response().setStatusCode(404).end("Not Found");
                }
            } if ("persistent".equals(pathSegments[0])) {
                if (pathSegments.length >= 4) {
                    String tenant = pathSegments[1];
                    String namespace = pathSegments[2];
                    String topic = pathSegments[3];
                    HttpMethod method = req.method();
                    if ("partitions".equals(pathSegments[pathSegments.length - 1])) {
                        if (method.equals(HttpMethod.PUT)) {
                            // Create partitioned topic
                            req.bodyHandler(buffer -> {
                                int numPartitions = buffer.toJsonObject().getInteger("numPartitions");
                                pulsarEngine.createPartitionedTopic(tenant, namespace, topic, numPartitions);
                                req.response().setStatusCode(204).end();
                            });
                        } else if (method.equals(HttpMethod.DELETE)) {
                            // Delete partitioned topic
                            pulsarEngine.deletePartitionedTopic(tenant, namespace, topic);
                            req.response().setStatusCode(204).end();
                        } else if (method.equals(HttpMethod.GET)) {
                            // Get partitioned topic metadata
                            PartitionedTopicInfo info = pulsarEngine.getPartitionedTopicInfo(tenant, namespace, topic);
                            if (info != null) {
                                req.response().setStatusCode(200).end(Json.encode(info));
                            } else {
                                req.response().setStatusCode(404).end("Partitioned topic not found");
                            }
                        } else {
                            req.response().setStatusCode(405).end("Method Not Allowed");
                        }
                    } else {
                        req.response().setStatusCode(404).end("Not Found");
                    }
                } else {
                    req.response().setStatusCode(404).end("Not Found");
                }
            } else {
                req.response().setStatusCode(404).end("Not Found");
            }
        });
        httpServer.listen(httpPort, res -> {
            if (res.succeeded()) {
                log.info("Embedded Pulsar HTTP server started on port {}", httpPort);
            } else {
                log.error("Failed to start Embedded Pulsar HTTP server", res.cause());
            }
        });
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
