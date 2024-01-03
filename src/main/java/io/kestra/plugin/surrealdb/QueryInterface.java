package io.kestra.plugin.surrealdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.common.FetchType;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

public interface QueryInterface {

	@Schema(
		title = "The way you want to store data.",
		description = "FETCH_ONE - output the first row.\n"
			+ "FETCH - output all rows as output variable.\n"
			+ "STORE - store all rows to a file.\n"
			+ "NONE - do nothing."
	)
	@PluginProperty
	@NotNull FetchType getFetchType();

	@Schema(
		title = "Query parameters, can be named parameters.",
		description = "See SurrealDB documentation about SurrealQL Prepared Statements for query syntax." +
			"This should be supplied with a parameter map using named parameters.",
		example = "my-field: my-value\n"+
			"my-second-field: my-second-value",
		allOf = {
			Map.class
		}
	)
	@PluginProperty(dynamic = true)
	Map<String, String> getParameters();

	@Schema(
		title = "SurrealQL query to execute."
	)
	@PluginProperty(dynamic = true)
	@NotBlank String getQuery();
}
