package io.kestra.plugin.surrealdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public interface SurrealDBConnectionInterface {
    int DEFAULT_PORT = 8000;

    @Schema(
        title = "Enable TLS for connection",
        description = "Use TLS when connecting to SurrealDB; default is `false`."
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getUseTls();

    @Schema(
        title = "Connection timeout",
        description = "Timeout for opening the connection in seconds; default is 60."
    )
    @PluginProperty(group = "execution")
    @Positive
    int getConnectionTimeout();

    @Schema(
        title = "Connection port",
        description = "TCP port of the SurrealDB endpoint, from 1 to 65535; default `8000`."
    )
    @PluginProperty(group = "connection")
    Property<@Min(1) @Max(65535) Integer> getPort();

    @Schema(
        title = "Connection host"
    )
    @PluginProperty(dynamic = true, group = "connection")
    @NotBlank
    String getHost();

    @Schema(
        title = "Plaintext authentication username"
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getUsername();

    @Schema(
        title = "Plaintext authentication password"
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getPassword();

    @Schema(
        title = "Connection namespace"
    )
    @PluginProperty(dynamic = true, group = "connection")
    @NotBlank
    String getNamespace();

    @Schema(
        title = "Connection database"
    )
    @PluginProperty(dynamic = true, group = "connection")
    @NotBlank
    String getDatabase();
}
