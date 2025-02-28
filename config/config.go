package config

import (
	"github.com/BurntSushi/toml"
)

func ReadClient(path string, config *Client) error {
	_, err := toml.DecodeFile(path, &config)
	return err
}

func ReadServer(path string, config *Server) error {
	_, err := toml.DecodeFile(path, &config)
	return err
}
