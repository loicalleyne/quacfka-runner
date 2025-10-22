package rpc

const (
	RESPONSE_OK                   = 0
	RESPONSE_NOT_AUTHORIZED       = 1
	RESPONSE_REQUEST_TYPE_INVALID = 2
	RESPONSE_RUNNER_ERROR         = 3
	RESPONSE_BUSY                 = 4
)

// The type Response is a struct containing an rpc response.
// Responses are sent to the client when a request is sent
// to the server.
type Response struct {
	// Request is the orginal request that was sent to the
	// server. We want to return the request in case the
	// client ever needs to use it.
	Request Request

	// Status is an integer that contains a status message
	// from the server to the client.
	Status int

	Error error
}

// The exported func ResponseNotAuthorized is called when the
// server tries to authenticate a request, but it can not be
// authenticated. This just returns a generic not authorized
// response for the client.
func ResponseNotAuthorized(request Request) Response {
	return Response{
		Request: request,
		Status:  RESPONSE_NOT_AUTHORIZED,
	}
}
