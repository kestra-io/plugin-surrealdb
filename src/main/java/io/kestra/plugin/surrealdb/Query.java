package io.kestra.plugin.surrealdb;

import com.surrealdb.driver.SyncSurrealDriver;
import com.surrealdb.driver.model.QueryResult;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
		title = "Query a Surreal database with N1QL."
)
public class Query extends SurrealDBConnection implements RunnableTask<Query.Output>, QueryInterface {

	@NotNull
	@Builder.Default
	protected FetchType fetchType = FetchType.STORE;
	protected Map<String, String> parameters;

	@NotNull
	@NotBlank
	protected String query;

	@Override
	public Query.Output run(RunContext runContext) throws Exception {
		SyncSurrealDriver driver = super.connect(runContext);

		String renderedQuery = runContext.render(query);

		List<QueryResult<Object>> results = driver.query(renderedQuery, parameters, Object.class);

		Output.OutputBuilder outputBuilder = Output.builder().size((long) results.stream().mapToLong(result -> (long) result.getResult().size()).sum());
		super.disconnect();

		return (
				switch (fetchType) {
					case FETCH -> outputBuilder.rows(results);
					case FETCH_ONE -> outputBuilder.row(results.stream().findFirst().orElse(null));
					case STORE -> {
						File tempFile = runContext.tempFile(".ion").toFile();
						BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
						try (OutputStream outputStream = new FileOutputStream(tempFile)) {
							FileSerde.write(outputStream, results);
						}

						fileWriter.flush();
						fileWriter.close();

						yield outputBuilder.uri(runContext.putTempFile(tempFile));
					}
					default -> outputBuilder;
				}
		             ).build();
	}

	@Builder
	@Getter
	public static class Output implements io.kestra.core.models.tasks.Output {
		@Schema(
				title = "List containing the fetched data",
				description = "Only populated if using `FETCH`."
		)
		private List<QueryResult<Object>> rows;

		@Schema(
				title = "Map containing the first row of fetched data",
				description = "Only populated if using `FETCH_ONE`."
		)
		private QueryResult<Object> row;

		@Schema(
				title = "The uri of the stored result",
				description = "Only populated if using `STORE`"
		)
		private URI uri;

		@Schema(
				title = "The amount of rows fetched"
		)
		private Long size;
	}
}
