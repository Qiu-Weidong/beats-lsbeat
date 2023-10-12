package main

import (
	"os"

	"github.com/Qiu-Weidong/lsbeat/cmd"

	_ "github.com/Qiu-Weidong/lsbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
