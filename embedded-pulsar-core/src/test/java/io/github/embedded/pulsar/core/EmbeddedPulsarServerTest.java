package io.github.embedded.pulsar.core;

import org.junit.jupiter.api.Disabled;
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
        config.serverKeyStorePath(this.getClass().getClassLoader().getResource("server.keystore.jks").getPath());
        config.serverKeyStorePassword("111111");
        config.serverTrustStorePath(this.getClass().getClassLoader().getResource("server.truststore.jks").getPath());
        config.serverTrustStorePassword("111111");
        config.clientKeyStorePath(this.getClass().getClassLoader().getResource("client.keystore.jks").getPath());
        config.clientKeyStorePassword("111111");
        config.clientTrustStorePath(this.getClass().getClassLoader().getResource("client.truststore.jks").getPath());
        config.clientTrustStorePassword("111111");
        EmbeddedPulsarServer server = new EmbeddedPulsarServer(config);
        server.start();
        server.close();
    }

}
