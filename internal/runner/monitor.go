package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
	"wgrot/v2/internal/sink"
)

type monitor struct {
	client   *http.Client
	requests []*http.Request

	tolerance int
	failed    int
}

func newMonitor(interval time.Duration) *monitor {
	urls := []string{
		"http://clients3.google.com/generate_204",
		"http://captive.apple.com/hotspot-detect.html",
		"http://detectportal.firefox.com/success.txt",
		"https://1.1.1.1/cdn-cgi/trace",
	}

	var reqs []*http.Request
	for _, u := range urls {
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			panic(fmt.Sprintf("create request: %v", err))
		}

		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Connection", "keep-alive")
		reqs = append(reqs, req)
	}

	client := http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        len(urls),
			MaxIdleConnsPerHost: 1,
			IdleConnTimeout:     interval + 15*time.Second,
			DisableKeepAlives:   false,
			ForceAttemptHTTP2:   true,
		},
	}

	return &monitor{
		client:    &client,
		requests:  reqs,
		failed:    0,
		tolerance: 5,
	}
}

func (m *monitor) IsConnected(ctx context.Context) bool {
	for _, req := range m.requests {
		if err := ctx.Err(); err != nil {
			sink.Printf(sink.ERROR, "context canceled: %v\n", err)
			return false
		}

		reqWithCtx := req.WithContext(ctx)
		resp, err := m.client.Do(reqWithCtx)
		if err != nil {
			sink.Printf(sink.TRACE, "%v\n", err)
			continue
		}

		status := resp.StatusCode
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if status == http.StatusOK || status == http.StatusNoContent {
			m.failed = 0
			return true
		}
	}

	m.failed++
	if m.failed <= m.tolerance {
		sink.Printf(sink.WARN, "test connection failed, %d/%d failures\n", m.failed, m.tolerance)
		return true
	}

	sink.Println(sink.ERROR, "test connection exceeded tolerance")
	return false
}
