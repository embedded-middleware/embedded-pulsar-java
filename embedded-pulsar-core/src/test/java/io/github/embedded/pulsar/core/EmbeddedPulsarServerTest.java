package io.github.embedded.pulsar.core;

import org.junit.jupiter.api.Test;

class EmbeddedPulsarServerTest {

    @Test
    public void testPulsarServerBoot() throws Exception {
        EmbeddedPulsarServer server = new EmbeddedPulsarServer();
        server.start();
        server.close();
    }

}
