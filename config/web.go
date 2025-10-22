package config

type Web struct {
	Host    string `json:"host" toml:"host"`
	Port    int    `json:"port" toml:"port"`
	Enabled bool   `json:"enabled" toml:"enabled"`
}
