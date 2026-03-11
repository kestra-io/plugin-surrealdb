package io.kestra.plugin.surrealdb;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.surrealdb.driver.SyncSurrealDriver;
import com.surrealdb.driver.model.QueryResult;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a SurrealDB query",
    description = "Executes a SurrealQL statement against a SurrealDB database. Defaults to `fetchType: STORE`, which streams rows to internal storage; use `FETCH` or `FETCH_ONE` to surface rows directly. TLS is off by default; keep credentials in secrets."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SurrealQL query to a SurrealDB database.",
            full = true,
            code = """
                id: surrealdb_query
                namespace: company.team

                tasks:
                  - id: select
                    type: io.kestra.plugin.surrealdb.Query
                    useTls: true
                    host: localhost
                    port: 8000
                    username: surreal_user
                    password: surreal_passwd
                    database: surreal_db
                    namespace: surreal_namespace
                    query: SELECT * FROM SURREAL_TABLE
                    fetchType: STORE
                """
        )
    }
)
public class Query extends SurrealDBConnection implements RunnableTask<Query.Output>, QueryInterface {

    @NotNull
    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Builder.Default
    protected Property<Map<String, String>> parameters = Property.ofValue(new HashMap<>());

    @NotBlank
    protected String query;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        SyncSurrealDriver driver = super.connect(runContext);

        String renderedQuery = runContext.render(query);

        Map<String, String> parametersValue = runContext.render(parameters).asMap(String.class, String.class).isEmpty() ? new HashMap<>()
            : runContext.render(parameters).asMap(String.class, String.class);
        List<QueryResult<Object>> results = driver.query(renderedQuery, parametersValue, Object.class);

        Query.Output.OutputBuilder outputBuilder = Output.builder().size(
            results.stream()
                .mapToLong(result -> result.getResult() != null ? (long) result.getResult().size() : (long) 0)
                .sum()
        );

        super.disconnect();

        return (switch (runContext.render(fetchType).as(FetchType.class).orElseThrow()) {
            case FETCH -> outputBuilder.rows(getResultStream(results).toList());
            case FETCH_ONE -> outputBuilder.row(getResultStream(results).findFirst().orElse(null));
            case STORE -> outputBuilder.uri(getTempFile(runContext, getResultStream(results).toList()));
            default -> outputBuilder;
        }).build();
    }

    private Stream<Map<String, Object>> getResultStream(List<QueryResult<Object>> results) {
        return results.stream()
            .map(QueryResult::getResult)
            .filter(Objects::nonNull)
            .flatMap(list -> list.stream().map(object -> (Map<String, Object>) object));
    }

    private URI getTempFile(RunContext runContext, List<Map<String, Object>> results) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
            var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
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
            title = "All fetched rows",
            description = "Populated only when `fetchType: FETCH`."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First fetched row",
            description = "Populated only when `fetchType: FETCH_ONE`."
        )
        private Map<String, Object> row;

        @Schema(
            title = "URI of stored result",
            description = "Internal storage URI populated only when `fetchType: STORE`."
        )
        private URI uri;

        @Schema(
            title = "Number of rows fetched"
        )
        private Long size;
    }
}
