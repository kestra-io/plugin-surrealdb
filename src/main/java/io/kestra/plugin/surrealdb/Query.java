package io.kestra.plugin.surrealdb;

import com.surrealdb.driver.SyncSurrealDriver;
import com.surrealdb.driver.model.QueryResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Query a SurrealDB database with SurrealQL.")
@Plugin(
	examples = {
		@Example(
			title = "Send a SurrealQL query to a SurrealDB database.",
			code = {
				"useTls: true",
				"port: 8000",
				"host: localhost",
				"username: surreal_user",
				"password: surreal_passwd",
				"database: surreal_db",
				"namespace: surreal_namespace",
				"query: SELECT * FROM SURREAL_TABLE",
				"fetchType: STORE"
			}
		)
	}
)
public class Query extends SurrealDBConnection implements RunnableTask<Query.Output>, QueryInterface {

	@NotNull
	@Builder.Default
	protected FetchType fetchType = FetchType.STORE;

	@Builder.Default
	protected Map<String, String> parameters = new HashMap<>();

	@NotBlank
	protected String query;

	@Override
	public Query.Output run(RunContext runContext) throws Exception {
		SyncSurrealDriver driver = super.connect(runContext);

		String renderedQuery = runContext.render(query);

		List<QueryResult<Object>> results = driver.query(renderedQuery, parameters, Object.class);

		Query.Output.OutputBuilder outputBuilder = Output.builder().size(results.stream()
			.mapToLong(result -> (long) result.getResult().size())
			.sum());

		super.disconnect();

		return (switch (fetchType) {
			case FETCH -> outputBuilder.rows(getResultStream(results).toList());
			case FETCH_ONE -> outputBuilder.row(getResultStream(results).findFirst().orElse(null));
			case STORE -> outputBuilder.uri(getTempFile(runContext, getResultStream(results).toList()));
			default -> outputBuilder;
		}).build();
	}

	private Stream<Map<String, Object>> getResultStream(List<QueryResult<Object>> results) {
		return results.stream()
			.map(QueryResult::getResult)
			.flatMap(list -> list.stream().map(object -> (Map<String, Object>) object));
	}

	private URI getTempFile(RunContext runContext, List<Map<String, Object>> results) throws IOException {
		File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
		try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
		    var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
			var flux = Flux.fromIterable(results);
			FileSerde.writeAll(output, flux).block();
			fileWriter.flush();
		}

		return runContext.storage().putFile(tempFile);
	}

	@Builder
	@Getter
	public static class Output implements io.kestra.core.models.tasks.Output {
		@Schema(
			title = "List containing the fetched data.",
			description = "Only populated if using `fetchType: FETCH`."
		)
		private List<Map<String, Object>> rows;

		@Schema(
			title = "Map containing the first row of fetched data.",
			description = "Only populated if using `fetchType: FETCH_ONE`."
		)
		private Map<String, Object> row;

		@Schema(
			title = "The URI of the stored result in Kestra's internal storage.",
			description = "Only populated if using `fetchType: STORE`."
		)
		private URI uri;

		@Schema(
			title = "The number of rows fetched."
		)
		private Long size;
	}
}
