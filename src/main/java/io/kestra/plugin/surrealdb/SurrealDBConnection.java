package io.kestra.plugin.surrealdb;

import com.surrealdb.connection.SurrealConnection;
import com.surrealdb.connection.SurrealWebSocketConnection;
import com.surrealdb.driver.AsyncSurrealDriver;
import com.surrealdb.driver.SyncSurrealDriver;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.util.concurrent.CompletableFuture;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class SurrealDBConnection extends Task implements SurrealDBConnectionInterface {

	private boolean useTls = false;

	@NotNull
	@Positive
	private int port;

	@NotNull
	@NotBlank
	private String host;

	@NotNull
	@NotBlank
	private String username;

	@NotNull
	@NotBlank
	private String password;

	@NotNull
	@NotBlank
	private String namespace;

	@NotNull
	@NotBlank
	private String database;

	private SurrealConnection connection;

	protected SyncSurrealDriver connect(RunContext context) throws IllegalVariableEvaluationException {
		SurrealWebSocketConnection connection = new SurrealWebSocketConnection(host, port, useTls);
		connection.connect(500);

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
		driver.signIn(context.render(username), context.render(password));
	}

}
