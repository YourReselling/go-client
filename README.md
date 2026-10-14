# Your-Reselling Go SDK

Go **1.22+**, nur Standardbibliothek. 599 konkrete Methoden, typisierte Requests, Context-Abbruch, streambare Uploads/Downloads und SSE. Der Modulpfad lautet `github.com/YourReselling/go-client/v3`.

Im konsumierenden Projekt installieren:

```sh
go get github.com/YourReselling/go-client/v3@v3.0.0
```

```go
package main

import (
    "context"
    "log"
    "os"
    reselling "github.com/YourReselling/go-client/v3"
)

func main() {
    client, err := reselling.NewClient(os.Getenv("RESELLING_API_KEY"), reselling.Options{})
    if err != nil { log.Fatal(err) }
    response, err := client.Rootserver.List(context.Background(), reselling.RootserverListRequest{
        Query: &reselling.RootserverListQuery{Page: reselling.Ptr(int64(1))},
    })
    if err != nil { log.Fatal(err) }
    defer response.Close()
    var payload map[string]any
    if err := response.Decode(&payload); err != nil { log.Fatal(err) }
}
```

`Options` akzeptiert `BaseURL` inklusive `/api/v1` oder `/v1`, `HTTPClient` und `Timeout` (Standard 120 Sekunden). Der übergebene HTTP-Client wird kopiert; Redirects sind für diesen SDK-Client deaktiviert. `context.WithTimeout` oder `WithCancel` begrenzt einzelne Requests. API-Keys bleiben serverseitig.

`Response` besitzt `StatusCode`, `Header`, `Body`, `Decode`, `WriteTo` und `Events`. Den Body immer mit `Close()` schließen; `Events` erledigt dies selbst. Ein leerer 204-Body ist beim Dekodieren kein Fehler. Ganze Antwortobjekte einschließlich Pagination bleiben erhalten. `Envelope[T]` ist optional und passt nicht zu allen Inference-Antworten.

`Upload{Reader: file, Filename: "audio.wav", ContentType: "audio/wav"}` streamt Multipart-Daten. `Inference.Create` ist der generierte Transkriptionsaufruf; `Inference.Completions` liefert bei `Stream: Ptr(true)` SSE. `response.Events(func(event Event) error { ... })` verarbeitet mehrzeilige Daten sowie OpenAI- und Anthropic-Events. Der Callback kann den Stream durch Rückgabe eines Fehlers beenden.

Optionale Skalare sind Zeiger, damit `false`/`0` von „nicht übergeben“ unterscheidbar bleiben. `Ptr` erleichtert ihre Verwendung. Für explizites JSON-`null`, unbekannte Felder oder neue serverseitige Erweiterungen kann `client.Do(ctx, method, path, query, body)` mit Maps verwendet werden. Die SDKs validieren keine providerabhängigen dynamischen Regeln vorab.

`APIError` per `errors.As` prüfen: `Status`, `Code`, `ValidationErrors`, `RequestID`, `RetryAfter`. Keine automatischen Retries, insbesondere keine erneute Bestellung nach unklarem Netzfehler. Preise können Dezimal-Strings sein; nicht ungeprüft in `float64` konvertieren.

```sh
go test -race ./...
go vet ./...
```

[Methodenverzeichnis](docs/api-methoden.md) · [OpenAPI](spec/openapi.json)

## Release-Paket bauen

Nach den lokalen Tests `python3 tools/package.py` ausführen. Das Skript verwendet nur dieses Repository und erzeugt das Paket sowie eine SHA-256-Prüfsumme unter `artifacts/`.
