package main

import (
	"os"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/jabbrwcky/tranquila/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var version = "dev"

type CLI struct {
	Config   string           `kong:"short='c',name='config',help='Config file path (YAML)',env='TRANQUILA_CONFIG'"`
	LogLevel string           `kong:"name='log-level',help='Log level (trace,debug,info,warn,error)',env='TRANQUILA_LOG_LEVEL',default='info'"`
	LogJSON  bool             `kong:"name='log-json',help='Output logs as JSON',env='TRANQUILA_LOG_JSON'"`
	Version  kong.VersionFlag `kong:"name='version',short='V',help='Show version and exit'"`

	Sync   SyncCmd   `kong:"cmd,default='',help='Synchronize S3 buckets (default)'"`
	Status StatusCmd `kong:"cmd,help='Show synchronization status'"`
}

func main() {
	cfgPath := findConfigPath(os.Args[1:])
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatal().Err(err).Str("path", cfgPath).Msg("failed to load config file")
	}

	cli := &CLI{}
	parser := kong.Must(cli,
		kong.Name("tranquila"),
		kong.Description("S3 bucket synchronization tool"),
		kong.Vars{"version": version},
		kong.Resolvers(config.NewResolver(cfg)),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)

	kctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	setupLogging(cli.LogLevel, cli.LogJSON)

	if err := kctx.Run(); err != nil {
		log.Fatal().Err(err).Msg("command failed")
	}
}

func findConfigPath(args []string) string {
	if v := os.Getenv("TRANQUILA_CONFIG"); v != "" {
		return v
	}
	for i, arg := range args {
		if (arg == "--config" || arg == "-c") && i+1 < len(args) {
			return args[i+1]
		}
		if s, ok := strings.CutPrefix(arg, "--config="); ok {
			return s
		}
		if s, ok := strings.CutPrefix(arg, "-c="); ok {
			return s
		}
	}
	return "tranquila.yaml"
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
