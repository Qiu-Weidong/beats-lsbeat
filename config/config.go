// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period time.Duration `config:"period"`

	RegistrarListPath string   `config:"registrar_list_path"`
	RegistrarLogPath  string   `config:"registrar_log_path"`
	Path              []string `config: "path"`
}

var DefaultConfig = Config{
	Period:            10 * time.Second,
	RegistrarListPath: "./data/registrar",
	RegistrarLogPath:  "./data/registrar",
	Path:              []string{},
}
