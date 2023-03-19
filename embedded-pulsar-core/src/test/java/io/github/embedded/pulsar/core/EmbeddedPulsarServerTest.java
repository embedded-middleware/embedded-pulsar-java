package io.github.embedded.pulsar.core;

import org.junit.jupiter.api.Test;

class EmbeddedPulsarServerTest {

    @Test
    public void testPulsarServerBoot() throws Exception {
        EmbeddedPulsarServer server = new EmbeddedPulsarServer();
        server.start();
        server.close();
    }

    @Test
    public void testTlsPulsarServerBoot() throws Exception {
        EmbeddedPulsarConfig config = new EmbeddedPulsarConfig();
        config.enableTls(true);
        config.serverKeyStorePath(this.getClass().getClassLoader().getResource("pulsar_server_key.jks").getPath());
        config.serverKeyStorePassword("pulsar_server_pwd");
        config.serverTrustStorePath(this.getClass().getClassLoader().getResource("pulsar_server_trust.jks").getPath());
        config.serverTrustStorePassword("pulsar_server_pwd");
        config.clientKeyStorePath(this.getClass().getClassLoader().getResource("pulsar_client_key.jks").getPath());
        config.clientKeyStorePassword("pulsar_client_pwd");
        config.clientTrustStorePath(this.getClass().getClassLoader().getResource("pulsar_client_trust.jks").getPath());
        config.clientTrustStorePassword("pulsar_client_pwd");
        EmbeddedPulsarServer server = new EmbeddedPulsarServer(config);
        server.start();
        server.close();
    }

}
