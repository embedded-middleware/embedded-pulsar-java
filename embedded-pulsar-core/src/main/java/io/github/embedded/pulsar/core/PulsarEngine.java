package io.github.embedded.pulsar.core;

import io.github.embedded.pulsar.core.module.NamespaceInfo;
import io.github.embedded.pulsar.core.module.PartitionedTopicInfo;
import io.github.embedded.pulsar.core.module.TenantInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PulsarEngine {
    private final ConcurrentHashMap<String, TenantInfo> tenants = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, NamespaceInfo>> namespaces = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, PartitionedTopicInfo>>> partitionedTopics = new ConcurrentHashMap<>();

    public void createTenant(String tenant, TenantInfo tenantInfo) {
        tenants.put(tenant, tenantInfo);
    }

    public void deleteTenant(String tenant) {
        tenants.remove(tenant);
    }

    public TenantInfo getTenantInfo(String tenant) {
        return tenants.get(tenant);
    }

    public boolean tenantExists(String tenant) {
        return tenants.containsKey(tenant);
    }

    public Set<String> getTenants() {
        return tenants.keySet();
    }

    public void createNamespace(String tenant, String namespace) {
        namespaces.computeIfAbsent(tenant, k -> new ConcurrentHashMap<>()).put(namespace, new NamespaceInfo());
    }

    public void deleteNamespace(String tenant, String namespace) {
        ConcurrentHashMap<String, NamespaceInfo> tenantNamespaces = namespaces.get(tenant);
        if (tenantNamespaces != null) {
            tenantNamespaces.remove(namespace);
        }
    }

    public List<String> getTenantNamespaces(String tenant) {
        ConcurrentHashMap<String, NamespaceInfo> tenantNamespaces = namespaces.get(tenant);
        return tenantNamespaces == null ? Collections.emptyList() : new ArrayList<>(tenantNamespaces.keySet());
    }

    public void createPartitionedTopic(String tenant, String namespace, String topic, int numPartitions) {
        partitionedTopics.computeIfAbsent(tenant, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                .put(topic, new PartitionedTopicInfo(numPartitions));
    }

    public void deletePartitionedTopic(String tenant, String namespace, String topic) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, PartitionedTopicInfo>> tenantNamespaces = partitionedTopics.get(tenant);
        if (tenantNamespaces != null) {
            ConcurrentHashMap<String, PartitionedTopicInfo> namespaceTopics = tenantNamespaces.get(namespace);
            if (namespaceTopics != null) {
                namespaceTopics.remove(topic);
            }
        }
    }

    public List<String> getPartitionedTopics(String tenant, String namespace) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, PartitionedTopicInfo>> tenantNamespaces = partitionedTopics.get(tenant);
        if (tenantNamespaces != null) {
            ConcurrentHashMap<String, PartitionedTopicInfo> namespaceTopics = tenantNamespaces.get(namespace);
            return namespaceTopics == null ? Collections.emptyList() : new ArrayList<>(namespaceTopics.keySet());
        }
        return Collections.emptyList();
    }

    public PartitionedTopicInfo getPartitionedTopicInfo(String tenant, String namespace, String topic) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, PartitionedTopicInfo>> tenantNamespaces = partitionedTopics.get(tenant);
        if (tenantNamespaces != null) {
            ConcurrentHashMap<String, PartitionedTopicInfo> namespaceTopics = tenantNamespaces.get(namespace);
            if (namespaceTopics != null) {
                return namespaceTopics.get(topic);
            }
        }
        return null;
    }
}
