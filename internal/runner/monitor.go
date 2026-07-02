package runner

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type monitor struct {
	client   *http.Client
	requests []*http.Request
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
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        len(urls),
			MaxIdleConnsPerHost: 1,
			IdleConnTimeout:     interval + 15*time.Second,
			DisableKeepAlives:   false,
			ForceAttemptHTTP2:   true,
		},
	}

	return &monitor{
		client:   &client,
		requests: reqs,
	}
}

func (m *monitor) IsConnected() bool {
	for _, req := range m.requests {
		resp, err := m.client.Do(req)
		if err != nil {
			continue
		}

		status := resp.StatusCode
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if status == http.StatusOK || status == http.StatusNoContent {
			return true
		}
	}

	return false
}
