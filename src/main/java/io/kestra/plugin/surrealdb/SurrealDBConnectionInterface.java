package io.kestra.plugin.surrealdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

public interface SurrealDBConnectionInterface {

	@Schema(
		title = "true to use TLS for connection. Default is false"
	)
	@PluginProperty
	boolean isUseTls();

	@Schema(
		title = "Connection timeout. Default is 60 seconds"
	)
	@PluginProperty
	@Positive
	int getConnectionTimeout();

	@Schema(
		title = "Connection port. Default value is 8000"
	)
	@PluginProperty
	@Positive
	int getPort();

	@Schema(
		title = "Connection host"
	)
	@PluginProperty(dynamic = true)
	@NotNull @NotBlank
	String getHost();

	@Schema(
		title = "Plaintext authentication username"
	)
	@PluginProperty(dynamic = true)
	@NotNull @NotBlank
	String getUsername();

	@Schema(
		title = "Plaintext authentication password"
	)
	@PluginProperty(dynamic = true)
	@NotNull @NotBlank
	String getPassword();

	@Schema(
		title = "Connection namespace"
	)
	@PluginProperty(dynamic = true)
	@NotNull @NotBlank
	String getNamespace();

	@Schema(
		title = "Connection database"
	)
	@PluginProperty(dynamic = true)
	@NotNull @NotBlank
	String getDatabase();
}
