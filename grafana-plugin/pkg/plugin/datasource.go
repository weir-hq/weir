// Package plugin is the backend of the Weir data source: it runs WeirQL queries against a Weir
// server and turns the results into data frames. Running on the backend makes alerting work.
package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
)

var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

type Datasource struct {
	url    string
	client *http.Client
	backend.CallResourceHandler
}

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	opts, err := settings.HTTPClientOptions(ctx)
	if err != nil {
		return nil, err
	}
	client, err := httpclient.New(opts)
	if err != nil {
		return nil, err
	}
	if settings.URL == "" {
		return nil, errors.New("the Weir server URL is not set")
	}
	d := &Datasource{url: strings.TrimRight(settings.URL, "/"), client: client}

	// Resources the query and variable editors call through Grafana.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /functions", d.proxy(func(*http.Request) string { return "/api/functions" }))
	mux.HandleFunc("GET /labels/{name}/values", d.proxy(func(r *http.Request) string {
		return "/api/labels/" + url.PathEscape(r.PathValue("name")) + "/values?" + r.URL.RawQuery
	}))
	d.CallResourceHandler = httpadapter.New(mux)
	return d, nil
}

func (d *Datasource) Dispose() {
	d.client.CloseIdleConnections()
}

// queryModel is what the query editor stores in the dashboard.
type queryModel struct {
	Expr string `json:"expr"`
}

func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	resp := backend.NewQueryDataResponse()
	for _, q := range req.Queries {
		resp.Responses[q.RefID] = d.query(ctx, q)
	}
	return resp, nil
}

func (d *Datasource) query(ctx context.Context, q backend.DataQuery) backend.DataResponse {
	var qm queryModel
	if err := json.Unmarshal(q.JSON, &qm); err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "bad query: "+err.Error())
	}
	if strings.TrimSpace(qm.Expr) == "" {
		return backend.DataResponse{}
	}
	params := url.Values{
		"q":    {qm.Expr},
		"from": {strconv.FormatInt(q.TimeRange.From.Unix(), 10)},
		"to":   {strconv.FormatInt(q.TimeRange.To.Unix(), 10)},
	}
	var res result
	status, err := d.get(ctx, "/api/query?"+params.Encode(), &res)
	if err != nil {
		return backend.ErrDataResponse(status, err.Error())
	}
	frames, err := toFrames(q.RefID, &res)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, err.Error())
	}
	return backend.DataResponse{Frames: frames}
}

func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var names []string
	if _, err := d.get(ctx, "/api/functions", &names); err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: "Cannot reach the Weir server: " + err.Error()}, nil
	}
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: fmt.Sprintf("Connected to Weir (%d query functions).", len(names)),
	}, nil
}

// get fetches a Weir API path and decodes its JSON body into v. Weir reports query errors as
// {"error": "..."} with status 400; those come back as the error text.
func (d *Datasource) get(ctx context.Context, path string, v any) (backend.Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.url+path, nil)
	if err != nil {
		return backend.StatusBadRequest, err
	}
	r, err := d.client.Do(req)
	if err != nil {
		return backend.StatusBadGateway, err
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		return backend.StatusBadGateway, err
	}
	if r.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return backend.Status(r.StatusCode), errors.New(e.Error)
		}
		return backend.Status(r.StatusCode), fmt.Errorf("weir server returned %s", r.Status)
	}
	if err := json.Unmarshal(body, v); err != nil {
		return backend.StatusBadGateway, fmt.Errorf("unexpected response from the Weir server: %w", err)
	}
	return backend.StatusOK, nil
}

// proxy forwards a resource call to a Weir API path and returns its JSON as is.
func (d *Datasource) proxy(path func(*http.Request) string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var v json.RawMessage
		status, err := d.get(r.Context(), path(r), &v)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(int(status))
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_, _ = w.Write(v)
	}
}
