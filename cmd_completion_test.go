package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

func TestCompleterUnsupportedShell(t *testing.T) {
	if _, err := completer("powershell"); err == nil {
		t.Error("expected an error for a shell kong's enum would never actually let through")
	}
}

func TestCompleterKnownShells(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish"} {
		t.Run(shell, func(t *testing.T) {
			c, err := completer(shell)
			if err != nil {
				t.Fatalf("completer(%q): %v", shell, err)
			}
			if c == nil {
				t.Fatalf("completer(%q) returned a nil Completer with no error", shell)
			}
		})
	}
}

// TestCompletionCmdRun drives the subcommand through a real kong Parse+Run,
// the same path main.go uses, rather than calling Run directly — the thing
// worth proving is that the *live* grammar (every flag, subcommand and enum
// already declared on CLI) round-trips through king correctly, not just that
// completer() returns a non-nil value.
func TestCompletionCmdRun(t *testing.T) {
	tests := []struct {
		shell string
		want  []string // substrings that must appear in the generated script
	}{
		{"bash", []string{
			"bash completion for tranquila",
			"_tranquila_completions",
			"sync",   // a real subcommand name
			"status", // a real subcommand name
		}},
		{"zsh", []string{
			"#compdef tranquila",
			"_tranquila_sync",
			"--source-endpoint", // a real, deeply-nested sync flag
		}},
		{"fish", []string{
			"fish shell completion for tranquila",
			"complete -c tranquila",
			"__fish_seen_subcommand_from sync",
		}},
	}

	for _, tc := range tests {
		t.Run(tc.shell, func(t *testing.T) {
			cli := &CLI{}
			var stdout bytes.Buffer
			parser := kong.Must(cli,
				kong.Name("tranquila"),
				kong.Writers(&stdout, &bytes.Buffer{}),
				kong.Exit(func(int) {}),
			)
			kctx, err := parser.Parse([]string{"completion", tc.shell})
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if err := kctx.Run(); err != nil {
				t.Fatalf("Run: %v", err)
			}

			out := stdout.String()
			if out == "" {
				t.Fatal("completion produced no output")
			}
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("output missing %q\nfull output:\n%s", want, out)
				}
			}
		})
	}
}

// A shell outside the enum must be rejected by kong's own flag parsing before
// CompletionCmd.Run ever sees it — this is the enum, not completer(), doing
// its job, and it must keep doing so since Run's own switch has no default
// error path a user-facing invocation could actually reach.
func TestCompletionCmdRejectsUnknownShell(t *testing.T) {
	cli := &CLI{}
	parser := kong.Must(cli,
		kong.Name("tranquila"),
		kong.Writers(&bytes.Buffer{}, &bytes.Buffer{}),
		kong.Exit(func(int) {}),
	)
	_, err := parser.Parse([]string{"completion", "powershell"})
	if err == nil {
		t.Fatal("expected kong's enum validation to reject an unsupported shell")
	}
}
