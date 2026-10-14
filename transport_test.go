package reselling

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fakeResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}
func testClient(t *testing.T, f roundTripFunc) *Client {
	t.Helper()
	client, err := NewClient("rsk_test_secret", Options{HTTPClient: &http.Client{Transport: f}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestAllRegisteredOperations(t *testing.T) {
	data, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Operations []struct {
			Method, Path, Group, Name string
			Authenticated             bool
			PathParameters            []struct{ Name string }
		}
	}
	if err = json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	title := func(s string) string {
		parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' })
		for i, part := range parts {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
		return strings.Join(parts, "")
	}
	for _, operation := range contract.Operations {
		t.Run(operation.Method+operation.Path, func(t *testing.T) {
			client := testClient(t, func(request *http.Request) (*http.Response, error) {
				expected := operation.Path
				for _, p := range operation.PathParameters {
					expected = strings.ReplaceAll(expected, "{"+p.Name+"}", "id%20with%20space")
				}
				if request.Method != operation.Method || request.URL.EscapedPath() != "/api/v1"+expected {
					t.Fatalf("wrong route: %s %s", request.Method, request.URL)
				}
				expectedAuth := ""
				if operation.Authenticated {
					expectedAuth = "Bearer rsk_test_secret"
				}
				if request.Header.Get("Authorization") != expectedAuth {
					t.Fatal("wrong authentication")
				}
				if request.Body != nil {
					_, _ = io.Copy(io.Discard, request.Body)
				}
				return fakeResponse(200, `{"success":true,"data":[],"meta":{"page":2}}`), nil
			})
			service := reflect.ValueOf(client).Elem().FieldByName(title(operation.Group))
			method := service.MethodByName(title(operation.Name))
			if !method.IsValid() {
				t.Fatal("missing method")
			}
			input := reflect.New(method.Type().In(1)).Elem()
			for _, p := range operation.PathParameters {
				input.FieldByName(title(p.Name)).SetString("id with space")
			}
			result := method.Call([]reflect.Value{reflect.ValueOf(context.Background()), input})
			if !result[1].IsNil() {
				t.Fatal(result[1].Interface())
			}
			response := result[0].Interface().(*Response)
			defer response.Close()
			var envelope map[string]any
			if err := response.Decode(&envelope); err != nil {
				t.Fatal(err)
			}
			if envelope["meta"] == nil {
				t.Fatal("lost pagination")
			}
		})
	}
}

func TestValidationErrorAndNoRetry(t *testing.T) {
	calls := 0
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		calls++
		r := fakeResponse(422, `{"error":{"code":"validation_error","message":"bad rsk_test_secret","errors":{"cores":["min1"]}}}`)
		r.Header.Set("X-Request-ID", "request-1")
		return r, nil
	})
	_, err := client.Rootserver.Create(context.Background(), RootserverCreateRequest{Cluster: "cluster"})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Status != 422 || apiError.RequestID != "request-1" || strings.Contains(err.Error(), "rsk_test_secret") {
		t.Fatalf("wrong error: %#v", err)
	}
	if calls != 1 {
		t.Fatal("retried order")
	}
}

func TestQueryMultipartAndStreaming(t *testing.T) {
	values, err := queryValues(map[string]any{"page": 0, "active": false, "filters": map[string]any{"status": []string{"running"}}, "nil": nil})
	if err != nil || values.Get("active") != "0" || values.Get("filters[status][0]") != "running" || values.Has("nil") {
		t.Fatalf("bad query: %v %v", values, err)
	}
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if err := request.ParseMultipartForm(1024); err != nil {
			t.Fatal(err)
		}
		file, _, err := request.FormFile("file")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		data, _ := io.ReadAll(file)
		if string(data) != "sound" || request.FormValue("model") != "example" {
			t.Fatal("bad multipart")
		}
		return fakeResponse(200, "text"), nil
	})
	response, err := client.transport.send(context.Background(), "POST", "/products/inference/audio/transcriptions", nil, nil, map[string]any{"file": &Upload{Reader: strings.NewReader("sound"), Filename: "sound.wav"}, "model": "example"}, true, true)
	if err != nil {
		t.Fatal(err)
	}
	response.Close()
	stream := &Response{Body: io.NopCloser(strings.NewReader(":ping\nevent: delta\ndata: one\ndata: two\n\ndata: [DONE]\n\n"))}
	count := 0
	if err := stream.Events(func(event Event) error {
		count++
		if event.Event != "delta" || event.Data != "one\ntwo" {
			t.Fatal(event)
		}
		return nil
	}); err != nil || count != 1 {
		t.Fatal(err, count)
	}
}

func TestSecurityAndCancellation(t *testing.T) {
	for _, base := range []string{"http://example.com/v1", "https://user@example.com/v1", "https://example.com/v1?secret=yes"} {
		if _, err := NewClient("token", Options{BaseURL: base}); err == nil {
			t.Fatal("unsafe base accepted")
		}
	}
	client := testClient(t, func(request *http.Request) (*http.Response, error) { return nil, request.Context().Err() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Account.Get(ctx, AccountGetRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := client.Rootserver.Get(context.Background(), RootserverGetRequest{Rootserver: ".."}); err == nil {
		t.Fatal("unsafe path accepted")
	}
}
