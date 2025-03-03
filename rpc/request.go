package rpc

const (
	REQUEST_PING     = 0
	REQUEST_RUN      = 1
	REQUEST_VALIDATE = 2
)

// The type Request is a struct containing an rpc request.
// Requests are sent to the server by the client.
type Request struct {
	// Type is an integer that contains the type of request
	// that is being sent from the client to the server.
	Type             int
	// Path to DuckDB file
	Path             string
	// Placeholder {{logname}} in Queries is replaced with this value.
	LogName          string
	// Placeholder {{exportpath}} in Queries is replaced with this value.
	ExportPath       string
	// Queries where no result is expected, or not requiring partitioning by year, month, day, hour.
	ExecQueries      []string
	// Names of exec queries. ExecQueries and ExecQueriesNames must have matching lengths.
	ExecQueriesNames []string
	// Query to retrieve values for partition by year, month, day, hour.
	PartitionQuery   string
	// Queries partitioned by year, month, day, hour.
	// Placeholders {{year}}, {{month}}, {{day}}, {{hour}} in queries will be replaced by values returned by PartitionQuery.
	Queries          []string
	// Names of queries with partitioning. Queries and QueriesNames length must match.
	QueriesNames     []string
}
