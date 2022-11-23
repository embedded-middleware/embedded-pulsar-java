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

    private boolean enableTls;

    private String serverKeyStorePath;

    private String serverKeyStorePassword;

    private String serverTrustStorePath;

    private String serverTrustStorePassword;

    private String clientKeyStorePath;

    private String clientKeyStorePassword;

    private String clientTrustStorePath;

    private String clientTrustStorePassword;

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

    public EmbeddedPulsarConfig enableTls(boolean enableTls) {
        this.enableTls = enableTls;
        return this;
    }

    public EmbeddedPulsarConfig serverKeyStorePath(String serverKeyStorePath) {
        this.serverKeyStorePath = serverKeyStorePath;
        return this;
    }

    public EmbeddedPulsarConfig serverKeyStorePassword(String serverKeyStorePassword) {
        this.serverKeyStorePassword = serverKeyStorePassword;
        return this;
    }

    public EmbeddedPulsarConfig serverTrustStorePath(String serverTrustStorePath) {
        this.serverTrustStorePath = serverTrustStorePath;
        return this;
    }

    public EmbeddedPulsarConfig serverTrustStorePassword(String serverTrustStorePassword) {
        this.serverTrustStorePassword = serverTrustStorePassword;
        return this;
    }

    public EmbeddedPulsarConfig clientKeyStorePath(String clientKeyStorePath) {
        this.clientKeyStorePath = clientKeyStorePath;
        return this;
    }

    public EmbeddedPulsarConfig clientKeyStorePassword(String clientKeyStorePassword) {
        this.clientKeyStorePassword = clientKeyStorePassword;
        return this;
    }

    public EmbeddedPulsarConfig clientTrustStorePath(String clientTrustStorePath) {
        this.clientTrustStorePath = clientTrustStorePath;
        return this;
    }

    public EmbeddedPulsarConfig clientTrustStorePassword(String clientTrustStorePassword) {
        this.clientTrustStorePassword = clientTrustStorePassword;
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
