package config

type Server struct {
	RPC       RPC `json:"rpc" toml:"rpc"`
	Web       Web `json:"web" toml:"web"`
	QueueSize int `json:"queue_size" toml:"queue_size"`
}
