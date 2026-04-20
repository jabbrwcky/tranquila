package main

import (
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	kongyaml "github.com/alecthomas/kong-yaml"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var version = "dev"

type CLI struct {
	ConfigFile kong.ConfigFlag  `json:"configFile" name:"configFile" short:"c" help:"Full path to a user-supplied config file"`
	LogLevel   string           `name:"log-level" help:"Log level (trace,debug,info,warn,error)" env:"TRANQUILA_LOG_LEVEL" default:"info"`
	LogJSON    bool             `name:"log-json" help:"Output logs as JSON" env:"TRANQUILA_LOG_JSON"`
	Version    kong.VersionFlag `name:"version" short:"V" help:"Show version and exit'"`
	Sync       SyncCmd          `cmd:"" default:"" help:"Synchronize S3 buckets (default)"`
	Status     StatusCmd        `cmd:"" help:"Show synchronization status"`
}

func main() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal().Err(err).Msg("unable to resolve home directory")
	}

	cli := &CLI{}
	parser := kong.Must(cli,
		kong.Configuration(kongyaml.Loader, filepath.Join(homeDir, ".config", "tranquila.yaml"), "tranquila.yaml"),
		kong.Name("tranquila"),
		kong.Description("S3 bucket synchronization tool"),
		kong.Vars{"version": version},
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)

	kctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	setupLogging(cli.LogLevel, cli.LogJSON)

	if err := kctx.Run(); err != nil {
		log.Fatal().Err(err).Msg("command failed")
	}
}

func setupLogging(level string, jsonOutput bool) {
	if !jsonOutput {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)
}
