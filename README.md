# llm-proxy

Single-user HTTP proxy for an LLM API endpoint. Forwards every request to
the configured endpoint, logs each request and response body to stdout, and
optionally rewrites string values in either direction.

## Build and run

```sh
cargo build --release
./target/release/llm-proxy [--raw] [config-path]
```

`config-path` defaults to `./config.toml`.

## Configuration

`config.toml`:

```toml
listen = "http://127.0.0.1:10000"      # HTTP only
endpoint = "https://api.anthropic.com" # HTTP or HTTPS

regex = ['s/find/replace/', 's/x/y/gi'] # optional, applies to both directions
```

The incoming path and query are appended to `endpoint`, so an endpoint of
`https://host/v1` prefixes every forwarded path.

Rule syntax, flags, and replacement syntax are documented in `config.toml`.

## Output

Normal mode prints each body under a `>>> REQUEST` or `<<< RESPONSE`
marker. JSON bodies are pretty-printed with a separate color palette per
direction. Strings containing newlines print as a flush-left block between
bare `"` lines. `<tag>...</tag>` spans inside strings print in bright white.
Colors are disabled when stdout is not a terminal.

`--raw` prints one JSON object per line and nothing else on stdout. Startup
and error messages go to stderr.

Empty bodies are not logged.

## Behavior

- Bodies are buffered in full before logging and forwarding. A streamed
  response is delivered to the client only after the upstream finishes.
- A `text/event-stream` response is reconstructed into the single message
  its events describe. That message is what gets logged. If no rule
  changes it, the original stream bytes are forwarded unchanged;
  otherwise a fresh event stream is generated from the rewritten message.
- Rules apply to every JSON string value in requests and responses alike,
  including tool-call inputs. Keys are never rewritten. A body that no rule changes is forwarded byte for
  byte; a changed body is re-serialized compactly with key order preserved.
- `accept-encoding` is stripped from forwarded requests so the upstream
  never compresses. Hop-by-hop headers and `host` are not forwarded.
- An upstream connection failure returns `502` to the client.
