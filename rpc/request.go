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
	Path             string
	LogName          string
	ExportPath       string
	ExecQueries      []string
	ExecQueriesNames []string
	PartitionQuery   string
	Queries          []string
	QueriesNames     []string
}
