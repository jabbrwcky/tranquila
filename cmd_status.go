package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pflege-de-labs/tranquila/internal/api"
)

type StatusCmd struct {
	Server  string   `kong:"name='server',env='MGMT_ADDR',default=':8080',help='Management API base URL, e.g. https://host:port. A bare host:port is assumed http://.'"`
	Buckets []string `kong:"arg,optional,help='Buckets to show (empty = all)'"`
}

func (cmd *StatusCmd) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	baseURL := cmd.Server
	if !strings.Contains(baseURL, "://") {
		baseURL = "http://" + baseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	var statuses []api.BucketStatus
	if len(cmd.Buckets) == 0 {
		var err error
		statuses, err = fetchBuckets(ctx, baseURL+"/api/v1/buckets")
		if err != nil {
			return err
		}
	} else {
		for _, name := range cmd.Buckets {
			bs, err := fetchBucket(ctx, baseURL+"/api/v1/buckets/"+url.PathEscape(name))
			if err != nil {
				return err
			}
			statuses = append(statuses, bs)
		}
	}

	return renderStatus(os.Stdout, statuses)
}

// renderStatus writes statuses as a tab-aligned table, factored out of Run so
// the formatting (which column shows which sentinel for "unknown") is
// testable without an HTTP round trip or a live management server.
func renderStatus(out io.Writer, statuses []api.BucketStatus) error {
	hasProgress := false
	for _, bs := range statuses {
		if bs.SyncProgress != nil {
			hasProgress = true
			break
		}
	}

	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
	if hasProgress {
		fmt.Fprintln(w, "BUCKET\tLAST COLLECTED\tTOTAL\tSYNCED\tPENDING\tFAILED\tPARKED\tRATE\tETA")
	} else {
		fmt.Fprintln(w, "BUCKET\tLAST COLLECTED\tTOTAL\tSYNCED\tPENDING\tFAILED\tPARKED")
	}

	for _, bs := range statuses {
		collected := "never"
		if bs.LastCollected != nil {
			collected = humanize.Time(*bs.LastCollected)
		}
		// "-" means the server has no source client wired to report this
		// (e.g. an older tranquila), distinct from a real "0" (genuinely
		// nothing parked).
		parked := "-"
		if bs.ParkedPrefixes != nil {
			parked = strconv.FormatInt(*bs.ParkedPrefixes, 10)
		}
		if hasProgress {
			rate, eta := "-", "-"
			if p := bs.SyncProgress; p != nil {
				rate = fmt.Sprintf("%.1f/s", p.RatePerSec)
				if p.ETASeconds != nil {
					eta = (time.Duration(*p.ETASeconds) * time.Second).String()
				}
			}
			fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n",
				bs.Name, collected,
				bs.Stats.Total, bs.Stats.Synced, bs.Stats.Pending, bs.Stats.Failed,
				parked, rate, eta)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%s\n",
				bs.Name, collected,
				bs.Stats.Total, bs.Stats.Synced, bs.Stats.Pending, bs.Stats.Failed,
				parked)
		}
	}
	return w.Flush()
}

func fetchBuckets(ctx context.Context, rawURL string) ([]api.BucketStatus, error) {
	body, err := doGet(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var result []api.BucketStatus
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

func fetchBucket(ctx context.Context, rawURL string) (api.BucketStatus, error) {
	body, err := doGet(ctx, rawURL)
	if err != nil {
		return api.BucketStatus{}, err
	}
	defer body.Close()
	var result api.BucketStatus
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return api.BucketStatus{}, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

func doGet(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", rawURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		var apiErr struct{ Error string }
		_ = json.NewDecoder(resp.Body).Decode(&apiErr)
		resp.Body.Close()
		if apiErr.Error != "" {
			return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, apiErr.Error)
		}
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, rawURL)
	}
	return resp.Body, nil
}
