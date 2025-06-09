package rcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type rcpDB struct {
	httpClient http.Client
	serverAddr string
	fieldcount int64
}

type GetValueResponse struct {
	Found bool   `json:"found"`
	Value string `json:"value"`
}

type rcpCreator struct{}

func init() {
	ycsb.RegisterDBCreator("rcp", rcpCreator{})
}

func (c rcpCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rcp := &rcpDB{}
	serverAddr, ok := p.Get("rcp.addr")
	if !ok {
		return nil, fmt.Errorf("property 'rcp.addr' must be specified")
	}
	rcp.serverAddr = "http://" + serverAddr

	client := http.Client{
		Timeout: 10 * time.Second,
	}

	rcp.httpClient = client
	rcp.fieldcount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	return rcp, nil
}

func (db *rcpDB) Delete(ctx context.Context, table string, key string) error {
	reqURL := fmt.Sprintf("%s/del?key=%s", db.serverAddr, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return err
	}
	res, err := db.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status code: %d", res.StatusCode)
	}
	return nil
}

func (db *rcpDB) CleanupThread(ctx context.Context) {}

func (db *rcpDB) Close() error {
	return nil
}

func (db *rcpDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (db *rcpDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	

	valueStringMap := make(map[string]string)

	for k, v := range values {
		valueStringMap[k] = string(v)
	}

	return db.insertStringStringMap(ctx, key, table, valueStringMap)
	
}

func (db *rcpDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {

	stringStringMap, err := db.getFieldValueMap(ctx, key, table)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %v", err)
	}

	data := make(map[string][]byte)
	for k, v := range stringStringMap {
		data[k] = []byte(v)
	}

	return data, err
}

func (db *rcpDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *rcpDB) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {

	fullUpdate := false
	if int64(len(values)) == db.fieldcount {
		fullUpdate = true
	}

	if fullUpdate {
		return db.Insert(ctx, table, key, values)
	} else {
		keyValueMap, err := db.getFieldValueMap(ctx, key, table)
		if err != nil {
			return err
		}
		for k, v := range values {
			keyValueMap[k] = string(v)
		}
		return db.insertStringStringMap(ctx, key, table, keyValueMap)
	}
}

func (db *rcpDB) getFieldValueMap(ctx context.Context, key, table string) (map[string]string, error) {
	baseURL := fmt.Sprintf("%s/get", db.serverAddr)

	params := url.Values{}
	params.Add("key", key)
	params.Add("bucket", table)

	reqURL := baseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := db.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	var getValResp GetValueResponse
	if err := json.NewDecoder(resp.Body).Decode(&getValResp); err != nil {
		return nil, fmt.Errorf("failed to decode json response: %w", err)
	}

	if !getValResp.Found {
		return nil, fmt.Errorf("key %s not found", key)
	}

	var stringStringMap map[string]string
	if err := json.Unmarshal([]byte(getValResp.Value), &stringStringMap); err != nil {
		return nil, fmt.Errorf("failed to decode inner JSON: %v", err)
	}

	return stringStringMap, nil
}

func (r *rcpDB) insertStringStringMap(ctx context.Context, key string, table string, valueStringMap map[string]string) error {
	baseURL := fmt.Sprintf("%s/put", r.serverAddr)
	valueBytes, err := json.Marshal(valueStringMap)
	if err != nil {
		return err
	}

	params := url.Values{}
	params.Add("key", key)
	params.Add("value", string(valueBytes))
	params.Add("bucket", table)

	reqURL := baseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	return nil
}
