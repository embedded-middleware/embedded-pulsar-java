package io.github.embedded.pulsar.core.module;


import lombok.Getter;
import lombok.Setter;

import java.util.Set;

@Getter
@Setter
public class TenantInfo {

    private Set<String> adminRoles;

    private Set<String> allowedClusters;

}
