package io.github.embedded.pulsar.core;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class EmbeddedPulsarConfig {

    private int bkPort;

    private int zkPort;

    private boolean allowAutoTopicCreation = false;

    private int autoCreateTopicPartitionNum = 2;

    private String autoTopicCreationType = "non-partitioned";

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

    public EmbeddedPulsarConfig allowAutoTopicCreation(boolean autoTopicCreation) {
        this.allowAutoTopicCreation = autoTopicCreation;
        return this;
    }

    public EmbeddedPulsarConfig autoCreateTopicPartitionNum(int autoCreateTopicPartitionNum) {
        this.autoCreateTopicPartitionNum = autoCreateTopicPartitionNum;
        return this;
    }

    public EmbeddedPulsarConfig autoTopicCreationType(AutoTopicCreationType autoTopicCreationType) {
        this.autoTopicCreationType = autoTopicCreationType.getValue();
        return this;
    }

    public enum AutoTopicCreationType {
        partitioned("partitioned"),
        non_partitioned("non-partitioned");

        private final String value;

        AutoTopicCreationType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

}
