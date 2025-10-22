package config

type RPC struct {
	Host string `json:"host" toml:"host"`
	Port int    `json:"port" toml:"port"`
}
