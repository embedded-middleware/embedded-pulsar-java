package io.github.embedded.pulsar.core;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class EmbeddedPulsarConfig {

    private int tcpPort = 6650;

    private int httpPort = 8080;

    public EmbeddedPulsarConfig() {
    }

    public EmbeddedPulsarConfig tcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
        return this;
    }

    public EmbeddedPulsarConfig httpPort(int httpPort) {
        this.httpPort = httpPort;
        return this;
    }

}
