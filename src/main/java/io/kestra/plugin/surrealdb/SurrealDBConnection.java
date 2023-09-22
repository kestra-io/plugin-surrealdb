package io.kestra.plugin.surrealdb;

import com.surrealdb.connection.SurrealConnection;
import com.surrealdb.connection.SurrealWebSocketConnection;
import com.surrealdb.driver.SyncSurrealDriver;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class SurrealDBConnection extends Task implements SurrealDBConnectionInterface {

	@Builder.Default
	private boolean useTls = false;

	@Positive
	@Builder.Default
	private int port = 8000;

	@NotBlank
	private String host;

	private String username;

	private String password;

	@NotBlank
	private String namespace;

	@NotBlank
	private String database;

	@Positive
	@Builder.Default
	private int connectionTimeout = 60;

	private SurrealConnection connection;

	protected SyncSurrealDriver connect(RunContext context) throws IllegalVariableEvaluationException {
		SurrealWebSocketConnection connection = new SurrealWebSocketConnection(context.render(host), port, useTls);
		connection.connect(connectionTimeout);

		SyncSurrealDriver driver = new SyncSurrealDriver(connection);

		signIn(driver, context);
		useDatabase(driver, context);

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
			driver.signIn(context.render(username), context.render(password));
		}
	}
}
