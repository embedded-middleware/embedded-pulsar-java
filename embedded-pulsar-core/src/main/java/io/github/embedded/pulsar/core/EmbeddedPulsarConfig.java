package io.github.embedded.pulsar.core;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class EmbeddedPulsarConfig {

    private int bkPort;

    private int zkPort;

    public EmbeddedPulsarConfig() {
    }

    public EmbeddedPulsarConfig bkPort(int bkPort) {
        this.bkPort = bkPort;
        return this;
    }

    public EmbeddedPulsarConfig zkPort(int zkPort) {
        this.zkPort = zkPort;
        return this;
    }

}
