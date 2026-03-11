package io.kestra.plugin.surrealdb;

import java.util.Map;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public interface QueryInterface {

    @Schema(
        title = "Result handling mode",
        description = "Controls how query results are returned. `FETCH_ONE` outputs the first row, `FETCH` outputs all rows, `STORE` writes rows to internal storage, `NONE` skips output creation."
    )
    @NotNull
    Property<FetchType> getFetchType();

    @Schema(
        title = "Named query parameters",
        description = "SurrealQL prepared-statement parameters rendered before execution. Provide a map of named placeholders to values.",
        example = "my-field: my-value\n" +
            "my-second-field: my-second-value",
        allOf = {
            Map.class
        }
    )
    Property<Map<String, String>> getParameters();

    @Schema(
        title = "SurrealQL query text"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    String getQuery();
}
