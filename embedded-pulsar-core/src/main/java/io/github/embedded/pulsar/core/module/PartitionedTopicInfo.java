package io.github.embedded.pulsar.core.module;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PartitionedTopicInfo {
    private int numPartitions;
}
