package config

type Server struct {
	RPC       RPC `json:"rpc" toml:"rpc"`
	Web       Web `json:"web" toml:"web"`
	QueueSize int `toml:"queue_size"`
}
