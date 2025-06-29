package io.kestra.plugin.surrealdb;

import com.surrealdb.connection.SurrealConnection;
import com.surrealdb.connection.SurrealWebSocketConnection;
import com.surrealdb.driver.SyncSurrealDriver;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class SurrealDBConnection extends Task implements SurrealDBConnectionInterface {

	@Builder.Default
	private Property<Boolean> useTls = Property.ofValue(false);

	@Positive
	@Builder.Default
	private int port = 8000;

	@NotBlank
	private String host;

	private Property<String> username;

	private Property<String> password;

	@NotBlank
	private String namespace;

	@NotBlank
	private String database;

	@Positive
	@Builder.Default
	private int connectionTimeout = 60;

	private SurrealConnection connection;

	protected SyncSurrealDriver connect(RunContext runContext) throws IllegalVariableEvaluationException {
		SurrealWebSocketConnection connection = new SurrealWebSocketConnection(runContext.render(host), port, runContext.render(useTls).as(Boolean.class).orElseThrow());
		connection.connect(connectionTimeout);

		SyncSurrealDriver driver = new SyncSurrealDriver(connection);

		signIn(driver, runContext);
		useDatabase(driver, runContext);

		return driver;
	}

	protected void disconnect() {
		if (this.connection != null) {
			connection.disconnect();
		}
	}

	private void useDatabase(SyncSurrealDriver driver, RunContext context) throws IllegalVariableEvaluationException {
		driver.use(context.render(namespace), context.render(database));
	}

	private void signIn(SyncSurrealDriver driver, RunContext context) throws IllegalVariableEvaluationException {
		if (username != null && password != null) {
			driver.signIn(context.render(username).as(String.class).orElseThrow(), context.render(password).as(String.class).orElseThrow());
		}
	}
}
