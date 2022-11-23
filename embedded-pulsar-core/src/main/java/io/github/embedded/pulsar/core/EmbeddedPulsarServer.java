package io.github.embedded.pulsar.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.PulsarStandalone;
import org.apache.pulsar.PulsarStandaloneBuilder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.assertj.core.util.Files;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EmbeddedPulsarServer {

    private final File bkDir;

    private final int bkPort;

    private final File zkDir;

    private final int zkPort;

    private final int webPort;

    private final int tcpPort;

    private final PulsarStandalone pulsarStandalone;

    private final EmbeddedPulsarConfig embeddedPulsarConfig;

    public EmbeddedPulsarServer() {
        this(new EmbeddedPulsarConfig());
    }

    public EmbeddedPulsarServer(EmbeddedPulsarConfig embeddedPulsarConfig) {
        this.embeddedPulsarConfig = embeddedPulsarConfig;
        try {
            this.bkDir = Files.newTemporaryFolder();
            this.bkDir.deleteOnExit();
            if (embeddedPulsarConfig.getBkPort() == 0) {
                this.bkPort = SocketUtil.getFreePort();
            } else {
                this.bkPort = embeddedPulsarConfig.getBkPort();
            }
            this.zkDir = Files.newTemporaryFolder();
            this.zkDir.deleteOnExit();
            if (embeddedPulsarConfig.getZkPort() == 0) {
                this.zkPort = SocketUtil.getFreePort();
            } else {
                this.zkPort = embeddedPulsarConfig.getZkPort();
            }
            LocalBookkeeperEnsemble bkEnsemble = new LocalBookkeeperEnsemble(
                    1, zkPort, bkPort, zkDir.toString(),
                    bkDir.toString(), false, "127.0.0.1");
            ServerConfiguration bkConf = new ServerConfiguration();
            bkConf.setJournalRemovePagesFromCache(false);
            log.info("begin to start bookkeeper");
            bkEnsemble.startStandalone(bkConf, false);
            this.webPort = SocketUtil.getFreePort();
            this.tcpPort = SocketUtil.getFreePort();
            this.pulsarStandalone = PulsarStandaloneBuilder
                    .instance()
                    .withZkPort(zkPort)
                    .withNumOfBk(1)
                    .withOnlyBroker(true)
                    .build();
            ServiceConfiguration standaloneConfig = this.pulsarStandalone.getConfig();
            standaloneConfig.setManagedLedgerDefaultEnsembleSize(1);
            standaloneConfig.setManagedLedgerDefaultWriteQuorum(1);
            standaloneConfig.setManagedLedgerDefaultAckQuorum(1);
            standaloneConfig.setAllowAutoTopicCreation(embeddedPulsarConfig.isAllowAutoTopicCreation());
            standaloneConfig.setAllowAutoTopicCreationType(embeddedPulsarConfig.getAutoTopicCreationType());
            standaloneConfig.setDefaultNumPartitions(embeddedPulsarConfig.getAutoCreateTopicPartitionNum());
            if (embeddedPulsarConfig.isEnableTls()) {
                standaloneConfig.setAuthenticationEnabled(embeddedPulsarConfig.isEnableTls());
                standaloneConfig.setWebServicePortTls(Optional.of(webPort));
                standaloneConfig.setBrokerServicePort(Optional.of(tcpPort));
                standaloneConfig.setTlsEnabledWithKeyStore(true);
                standaloneConfig.setTlsKeyStore(embeddedPulsarConfig.getServerKeyStorePath());
                standaloneConfig.setTlsKeyStorePassword(embeddedPulsarConfig.getServerKeyStorePassword());
                standaloneConfig.setTlsTrustStore(embeddedPulsarConfig.getServerTrustStorePath());
                standaloneConfig.setTlsTrustStorePassword(embeddedPulsarConfig.getServerTrustStorePassword());
                Map<String, String> map = new HashMap<>();
                map.put("keyStoreType", "JKS");
                map.put("keyStorePath", embeddedPulsarConfig.getClientKeyStorePath());
                map.put("keyStorePassword", embeddedPulsarConfig.getClientKeyStorePassword());
                standaloneConfig.setBrokerClientAuthenticationParameters(new ObjectMapper().writeValueAsString(map));
                standaloneConfig.setBrokerClientAuthenticationPlugin(AuthenticationKeyStoreTls.class.getName());
                standaloneConfig.setBrokerClientTlsTrustStore(embeddedPulsarConfig.getClientTrustStorePath());
                standaloneConfig.setBrokerClientTlsTrustStorePassword(embeddedPulsarConfig.getClientTrustStorePassword());
            } else {
                standaloneConfig.setWebServicePort(Optional.of(webPort));
                standaloneConfig.setBrokerServicePort(Optional.of(tcpPort));
            }
            this.pulsarStandalone.setConfig(standaloneConfig);
        } catch (Throwable e) {
            log.error("exception is ", e);
            throw new IllegalStateException("start pulsar standalone failed");
        }
    }

    public void start() throws Exception {
        this.pulsarStandalone.start();
        long start = System.nanoTime();
        PulsarAdmin admin = null;
        while (true) {
            try {
                admin = createPulsarAdmin();
                admin.brokers().healthcheck(TopicVersion.V1);
                log.info("started pulsar");
                admin.close();
                break;
            } catch (Exception e) {
                if (System.nanoTime() - start > TimeUnit.MINUTES.toNanos(3)) {
                    log.error("start pulsar timeout, stopping pulsar");
                    this.pulsarStandalone.close();
                    if (admin != null) {
                        admin.close();
                    }
                    break;
                }
                log.info("starting pulsar....");
                TimeUnit.SECONDS.sleep(10);
            }
        }
    }

    public PulsarAdmin createPulsarAdmin() throws PulsarClientException {
        PulsarAdminBuilder builder = PulsarAdmin.builder();
        String serviceHttpUrl;
        if (embeddedPulsarConfig.isEnableTls()) {
            serviceHttpUrl = "https://localhost:" + this.webPort;
            Map<String, String> map = new HashMap<>();
            map.put("keyStoreType", "JKS");
            map.put("keyStorePath", embeddedPulsarConfig.getClientKeyStorePath());
            map.put("keyStorePassword", embeddedPulsarConfig.getClientKeyStorePassword());
            Authentication authentication = AuthenticationFactory
                    .create(AuthenticationKeyStoreTls.class.getName(), map);
            builder.allowTlsInsecureConnection(true);
            builder.enableTlsHostnameVerification(false);
            builder.authentication(authentication);
        } else {
            serviceHttpUrl = "http://localhost:" + this.webPort;
        }
        return builder.serviceHttpUrl(serviceHttpUrl).build();
    }

    public int getWebPort() {
        return webPort;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public void close() throws Exception {
        this.pulsarStandalone.close();
    }
}
