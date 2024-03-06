package io.github.embedded.pulsar.core;

public class EmbeddedPulsarServerExample {
    public static void main(String[] args) throws Exception {
        EmbeddedPulsarConfig pulsarConfig = new EmbeddedPulsarConfig();
        EmbeddedPulsarServer embeddedPulsarServer = new EmbeddedPulsarServer(pulsarConfig);
        embeddedPulsarServer.start();
    }
}
