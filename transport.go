// Package reselling provides the registered reseller API with context cancellation,
// typed request payloads, multipart uploads, and unmodified response envelopes.
package reselling

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Options controls transport; BaseURL includes /api/v1 (or /v1 on the API host).
type Options struct {
	BaseURL    string
	HTTPClient *http.Client
	Timeout    time.Duration
}

// APIError never contains the API token or raw gateway HTML.
type APIError struct {
	Status           int
	Code             string
	Message          string
	ValidationErrors map[string]any
	RequestID        string
	RetryAfter       string
}

func (e *APIError) Error() string { return fmt.Sprintf("API HTTP %d: %s", e.Status, e.Message) }

// Response owns a streaming body. Always Close it, including after Decode.
// The body preserves all envelope fields and pagination metadata.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

func (r *Response) Close() error { return r.Body.Close() }
func (r *Response) Decode(destination any) error {
	err := json.NewDecoder(r.Body).Decode(destination)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
func (r *Response) WriteTo(destination io.Writer) (int64, error) { return io.Copy(destination, r.Body) }

// Envelope is optional: inference and some endpoints use their own JSON structure.
type Envelope[T any] struct {
	Success bool            `json:"success"`
	Data    T               `json:"data"`
	Meta    json.RawMessage `json:"meta,omitempty"`
	Message string          `json:"message,omitempty"`
}

// Upload streams a file without reading it completely into memory.
type Upload struct {
	Reader      io.Reader
	Filename    string
	ContentType string
}

// Event is one SSE event. Data is raw text; callers may JSON-decode it as needed.
type Event struct {
	Event string
	Data  string
	ID    string
}

// Events consumes an SSE response until EOF, [DONE], cancellation, or callback error.
func (r *Response) Events(consume func(Event) error) error {
	defer r.Close()
	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 4096), 16*1024*1024)
	event := Event{Event: "message"}
	var data []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(data) > 0 {
				event.Data = strings.Join(data, "\n")
				if event.Data == "[DONE]" {
					return nil
				}
				if err := consume(event); err != nil {
					return err
				}
			}
			event.Event = "message"
			data = nil
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		key, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch key {
		case "data":
			data = append(data, value)
		case "event":
			event.Event = value
		case "id":
			if !strings.ContainsRune(value, 0) {
				event.ID = value
			}
		}
	}
	return scanner.Err()
}

type transport struct {
	token   string
	baseURL string
	http    *http.Client
}

func newTransport(token string, options Options) (*transport, error) {
	base := options.BaseURL
	if base == "" {
		base = "https://resellingprovider.de/api/v1"
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("API base URL must use HTTPS without credentials, query or fragment")
	}
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("a non-empty API token without newline characters is required")
	}
	client := http.Client{Timeout: 120 * time.Second}
	if options.HTTPClient != nil {
		client = *options.HTTPClient
	}
	if options.Timeout > 0 {
		client.Timeout = options.Timeout
	}
	if client.Timeout == 0 {
		client.Timeout = 120 * time.Second
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &transport{token: token, baseURL: strings.TrimRight(base, "/"), http: &client}, nil
}

func queryValues(value any) (url.Values, error) {
	output := url.Values{}
	if value == nil {
		return output, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err = decoder.Decode(&fields); err != nil {
		return nil, err
	}
	var add func(string, any)
	add = func(key string, v any) {
		switch x := v.(type) {
		case nil:
			return
		case bool:
			if x {
				output.Add(key, "1")
			} else {
				output.Add(key, "0")
			}
		case []any:
			for i, item := range x {
				add(key+"["+strconv.Itoa(i)+"]", item)
			}
		case map[string]any:
			for child, item := range x {
				add(key+"["+child+"]", item)
			}
		default:
			output.Add(key, fmt.Sprint(x))
		}
	}
	for key, item := range fields {
		add(key, item)
	}
	return output, nil
}

func multipartValue(writer *multipart.Writer, name string, value reflect.Value) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		if upload, ok := value.Interface().(*Upload); ok {
			return writeUpload(writer, name, upload)
		}
		return multipartValue(writer, name, value.Elem())
	}
	if upload, ok := value.Interface().(Upload); ok {
		return writeUpload(writer, name, &upload)
	}
	switch value.Kind() {
	case reflect.Struct:
		typ := value.Type()
		for i := 0; i < value.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			key, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if key == "-" {
				continue
			}
			if key == "" {
				key = field.Name
			}
			if name != "" {
				key = name + "[" + key + "]"
			}
			if err := multipartValue(writer, key, value.Field(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			child := fmt.Sprint(key.Interface())
			if name != "" {
				child = name + "[" + child + "]"
			}
			if err := multipartValue(writer, child, value.MapIndex(key)); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := multipartValue(writer, fmt.Sprintf("%s[%d]", name, i), value.Index(i)); err != nil {
				return err
			}
		}
	default:
		text := fmt.Sprint(value.Interface())
		if value.Kind() == reflect.Bool {
			if value.Bool() {
				text = "1"
			} else {
				text = "0"
			}
		}
		return writer.WriteField(name, text)
	}
	return nil
}

func writeUpload(writer *multipart.Writer, name string, upload *Upload) error {
	if upload.Reader == nil {
		return errors.New("upload Reader is required")
	}
	if strings.ContainsAny(name+upload.Filename+upload.ContentType, "\r\n") {
		return errors.New("invalid upload filename or content type")
	}
	escape := strings.NewReplacer("\\", "\\\\", "\"", "\\\"")
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, escape.Replace(name), escape.Replace(upload.Filename)))
	contentType := upload.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	header.Set("Content-Type", contentType)
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(part, upload.Reader)
	return err
}

// send never retries, because the API does not promise idempotent product orders.
func (t *transport) send(ctx context.Context, method, path string, pathParams map[string]string, query, body any, multipartBody, authenticated bool) (*Response, error) {
	for key, value := range pathParams {
		if value == "" || value == "." || value == ".." {
			return nil, errors.New("path parameters must be non-empty identifiers")
		}
		path = strings.ReplaceAll(path, "{"+key+"}", url.PathEscape(value))
	}
	path = strings.TrimLeft(path, "/")
	if path == "" || strings.ContainsAny(path, "{}?#") || strings.Contains(path, "://") {
		return nil, errors.New("invalid API path or missing route parameter")
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return nil, errors.New("invalid API path")
		}
	}
	values, err := queryValues(query)
	if err != nil {
		return nil, err
	}
	endpoint := t.baseURL + "/" + path
	if len(values) > 0 {
		endpoint += "?" + values.Encode()
	}
	var reader io.Reader
	var pipe *io.PipeReader
	contentType := "application/json"
	if body != nil {
		if multipartBody {
			pr, pw := io.Pipe()
			pipe = pr
			writer := multipart.NewWriter(pw)
			contentType = writer.FormDataContentType()
			reader = pr
			go func() {
				err := multipartValue(writer, "", reflect.ValueOf(body))
				if err == nil {
					err = writer.Close()
				}
				_ = pw.CloseWithError(err)
			}()
		} else {
			data, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			reader = bytes.NewReader(data)
		}
	}
	if pipe != nil {
		defer pipe.Close()
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "YourReselling-Go/3.0")
	if authenticated {
		request.Header.Set("Authorization", "Bearer "+t.token)
	}
	if body != nil {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := t.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("API transport failed; check connectivity and TLS, and do not automatically retry orders")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		var payload map[string]any
		_ = json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&payload)
		detail, _ := payload["error"].(map[string]any)
		message, _ := detail["message"].(string)
		if message == "" {
			message, _ = payload["message"].(string)
		}
		if message == "" {
			message = "API request failed"
		}
		code, _ := detail["code"].(string)
		validation, _ := detail["errors"].(map[string]any)
		if validation == nil {
			validation, _ = payload["errors"].(map[string]any)
		}
		retry := response.Header.Get("Retry-After")
		if retry == "" && detail["retry_after"] != nil {
			retry = fmt.Sprint(detail["retry_after"])
		}
		return nil, &APIError{Status: response.StatusCode, Code: code, Message: strings.ReplaceAll(message, t.token, "[redacted]"), ValidationErrors: validation, RequestID: response.Header.Get("X-Request-ID"), RetryAfter: retry}
	}
	return &Response{StatusCode: response.StatusCode, Header: response.Header, Body: response.Body}, nil
}

// Ptr constructs an optional field while preserving false, zero and empty strings.
func Ptr[T any](value T) *T { return &value }
