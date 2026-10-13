use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpListener;
use url::Url;

mod stream;
mod transform;

// Unknown keys are rejected so a misplaced or misspelled key fails at
// startup instead of being silently ignored.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    listen: String,
    endpoint: String,
    #[serde(default)]
    regex: Vec<String>,
}

struct Proxy {
    client: reqwest::Client,
    endpoint: Url,
    // Rewrite rules applied to every string value of every body, in both
    // directions, before it is logged and forwarded. Empty means bodies pass
    // through byte for byte.
    rules: Vec<transform::Rule>,
    // Decided once at startup from whether stdout is a terminal. When false
    // no escape codes are emitted anywhere.
    color: bool,
    // `--raw`: stdout carries only payload bytes, one per line, and every
    // diagnostic line goes to stderr instead.
    raw: bool,
}

impl Proxy {
    // Non-payload output: startup notice and error lines.
    fn diag(&self, line: &str) {
        if self.raw {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    }
}

// Which side of the exchange a logged payload belongs to. Each direction has
// its own key and string colors so request and response bodies are
// distinguishable at a glance, not only by the marker line.
enum Direction {
    Request,
    Response { ok: bool },
}

// SGR parameters for the elements of one direction's palette. Numbers,
// booleans, null and punctuation use the terminal default in every palette.
struct Palette {
    marker: &'static str,
    key: &'static str,
    string: &'static str,
}

// Bright white, used for `<tag>...</tag>` spans inside strings in every
// palette. Chosen so it stands out from both directions' string colors.
const TAG_SGR: &str = "97";

impl Direction {
    fn marker(&self) -> &'static str {
        match self {
            Direction::Request => ">>> REQUEST",
            Direction::Response { ok: true } => "<<< RESPONSE",
            Direction::Response { ok: false } => "<<< RESPONSE (error)",
        }
    }

    fn palette(&self) -> Palette {
        match self {
            Direction::Request => Palette {
                marker: "1;34",
                key: "1;34",
                string: "32",
            },
            Direction::Response { ok: true } => Palette {
                marker: "1;35",
                key: "1;35",
                string: "33",
            },
            Direction::Response { ok: false } => Palette {
                marker: "1;31",
                key: "1;31",
                string: "33",
            },
        }
    }
}

// Pretty-printer for a parsed JSON value. Mirrors jq's layout (two-space
// indent, one item per line) with two deviations chosen for reading LLM
// payloads: strings containing a newline are printed as a raw flush-left
// block between bare `"` lines, and `<tag>...</tag>` spans inside strings are
// highlighted.
struct Printer<'a> {
    out: String,
    palette: &'a Palette,
    color: bool,
}

impl Printer<'_> {
    fn sgr(&mut self, code: &str) {
        if self.color {
            self.out.push_str("\x1b[");
            self.out.push_str(code);
            self.out.push('m');
        }
    }

    fn reset(&mut self) {
        if self.color {
            self.out.push_str("\x1b[0m");
        }
    }

    fn indent(&mut self, depth: usize) {
        for _ in 0..depth {
            self.out.push_str("  ");
        }
    }

    fn value(&mut self, value: &serde_json::Value, depth: usize) {
        use serde_json::Value;
        match value {
            Value::Null => self.out.push_str("null"),
            Value::Bool(b) => self.out.push_str(if *b { "true" } else { "false" }),
            Value::Number(n) => self.out.push_str(&n.to_string()),
            Value::String(s) => self.string(s),
            Value::Array(items) => {
                if items.is_empty() {
                    self.out.push_str("[]");
                    return;
                }
                self.out.push_str("[\n");
                for (i, item) in items.iter().enumerate() {
                    // A multi-line string element opens on its own line at
                    // column 0, so indenting first would leave a line of
                    // trailing spaces.
                    if !is_multiline(item) {
                        self.indent(depth + 1);
                    }
                    self.value(item, depth + 1);
                    self.separator(i + 1 < items.len());
                }
                self.indent(depth);
                self.out.push(']');
            }
            Value::Object(fields) => {
                if fields.is_empty() {
                    self.out.push_str("{}");
                    return;
                }
                self.out.push_str("{\n");
                for (i, (key, item)) in fields.iter().enumerate() {
                    self.indent(depth + 1);
                    let key_sgr = self.palette.key;
                    self.sgr(key_sgr);
                    self.out.push('"');
                    self.out.push_str(&escape(key));
                    self.out.push('"');
                    self.reset();
                    self.out.push(':');
                    // A multi-line string starts on its own line at column 0
                    // so the block's left edge is the text's own indentation.
                    if !is_multiline(item) {
                        self.out.push(' ');
                    }
                    self.value(item, depth + 1);
                    self.separator(i + 1 < fields.len());
                }
                self.indent(depth);
                self.out.push('}');
            }
        }
    }

    fn separator(&mut self, more: bool) {
        if more {
            self.out.push(',');
        }
        self.out.push('\n');
    }

    fn string(&mut self, s: &str) {
        let string_sgr = self.palette.string;
        if s.contains('\n') {
            // Raw block: escapes are decoded, the text is printed verbatim
            // between bare quote lines. The trailing newline before the
            // closing quote is only added when the text does not end with one.
            self.out.push('\n');
            self.sgr(string_sgr);
            self.out.push_str("\"\n");
            self.highlighted(s);
            if !s.ends_with('\n') {
                self.out.push('\n');
            }
            self.sgr(string_sgr);
            self.out.push('"');
            self.reset();
        } else {
            self.sgr(string_sgr);
            self.out.push('"');
            self.highlighted(&escape(s));
            self.sgr(string_sgr);
            self.out.push('"');
            self.reset();
        }
    }

    // Writes `text` in the string color, switching to TAG_SGR for each
    // `<name>...</name>` span. Leaves the string color active on return.
    fn highlighted(&mut self, text: &str) {
        let string_sgr = self.palette.string;
        let mut rest = text;
        while let Some((before, span, after)) = next_tag_span(rest) {
            self.sgr(string_sgr);
            self.out.push_str(before);
            self.sgr(TAG_SGR);
            self.out.push_str(span);
            rest = after;
        }
        self.sgr(string_sgr);
        self.out.push_str(rest);
    }
}

// Finds the first `<name>...</name>` span in `text`. Returns the text before
// it, the span itself (tags included), and the text after. An opening tag
// with no matching close is skipped and the search continues past it.
fn next_tag_span(text: &str) -> Option<(&str, &str, &str)> {
    let mut from = 0;
    while let Some(rel) = text[from..].find('<') {
        let open = from + rel;
        let name_start = open + 1;
        let name_len = text[name_start..]
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
            .count();
        if name_len > 0 && text[name_start + name_len..].starts_with('>') {
            let name = &text[name_start..name_start + name_len];
            let body_start = name_start + name_len + 1;
            let close = format!("</{name}>");
            if let Some(rel_close) = text[body_start..].find(&close) {
                let end = body_start + rel_close + close.len();
                return Some((&text[..open], &text[open..end], &text[end..]));
            }
        }
        from = open + 1;
    }
    None
}

fn is_multiline(value: &serde_json::Value) -> bool {
    matches!(value, serde_json::Value::String(s) if s.contains('\n'))
}

// JSON string escaping for single-line output, matching serde_json's
// serializer so single-line strings look exactly as jq prints them.
fn escape(s: &str) -> String {
    let quoted = serde_json::to_string(s).expect("serializing a str");
    // Slice rather than trim: a string ending in an escaped quote would lose
    // the escaped character to trim_matches.
    quoted[1..quoted.len() - 1].to_owned()
}

fn render_json(value: &serde_json::Value, palette: &Palette, color: bool) -> String {
    let mut printer = Printer {
        out: String::new(),
        palette,
        color,
    };
    printer.value(value, 0);
    printer.out
}

// Hop-by-hop headers describe a single connection, not the message, so they
// must not be forwarded across the proxy. reqwest and hyper each manage their
// own connection-level headers. `Host` is included because reqwest derives it
// from the upstream URL; forwarding the client's `Host` would send the
// listen address to the upstream. `Accept-Encoding` is stripped so the
// upstream never compresses: the proxy must be able to read every body, and
// reqwest is built without decompression.
const HOP_BY_HOP: &[&str] = &[
    "accept-encoding",
    "connection",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
];

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|h| h.eq_ignore_ascii_case(name))
}

// What gets logged for one direction. `Bytes` is a body exactly as it was on
// the wire; `Json` is a message the proxy assembled from an event stream,
// which never existed as a single object on the wire.
enum Payload<'a> {
    Bytes(&'a [u8]),
    Json(&'a serde_json::Value),
}

// Applies `rules` to a JSON body. Returns the original bytes untouched when
// there are no rules, the body is not JSON, or nothing matched; otherwise the
// rewritten object re-serialized compactly with key order preserved.
fn rewrite(rules: &[transform::Rule], body: Bytes) -> Bytes {
    if rules.is_empty() {
        return body;
    }
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return body;
    };
    if transform::apply_rules(rules, &mut value) {
        Bytes::from(serde_json::to_vec(&value).expect("serializing a Value"))
    } else {
        body
    }
}

// Only payloads are logged; empty bodies produce no output at all. JSON
// bodies are pretty-printed jq-style. Anything that fails to parse as a
// single JSON document is printed verbatim.
//
// In raw mode each payload is one line: wire bytes are written untouched,
// and a reconstructed message is serialized compactly. Either way one
// trailing newline separates consecutive payloads for tools like `jq -c`.
fn log_body(proxy: &Proxy, direction: Direction, payload: Payload) {
    if matches!(payload, Payload::Bytes(b) if b.is_empty()) {
        return;
    }
    if proxy.raw {
        use std::io::Write;
        let line = match &payload {
            Payload::Bytes(b) => Bytes::copy_from_slice(b),
            Payload::Json(v) => {
                Bytes::from(serde_json::to_vec(v).expect("serializing a Value"))
            }
        };
        let mut out = std::io::stdout().lock();
        // Ignore write errors: a closed pipe on stdout must not break proxying.
        let _ = out.write_all(&line).and_then(|_| out.write_all(b"\n"));
        return;
    }
    let color = proxy.color;
    let palette = direction.palette();
    let rendered = match payload {
        Payload::Json(value) => render_json(value, &palette, color),
        Payload::Bytes(body) => match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(value) => render_json(&value, &palette, color),
            Err(_) => match std::str::from_utf8(body) {
                Ok(text) => text.to_owned(),
                Err(_) => format!("<{} bytes of non-UTF-8 body>", body.len()),
            },
        },
    };
    let marker = if color {
        format!("\x1b[{}m{}\x1b[0m", palette.marker, direction.marker())
    } else {
        direction.marker().to_owned()
    };
    println!("{marker}\n{rendered}");
}

async fn handle(proxy: Arc<Proxy>, req: Request<Incoming>) -> Result<Response<Full<Bytes>>> {
    let (parts, body) = req.into_parts();
    let body = body.collect().await.context("reading client body")?.to_bytes();

    // Rebuild the target URL from the endpoint base plus the incoming
    // path-and-query, so an endpoint like "https://host/v1" prefixes every
    // forwarded path.
    let mut target = proxy.endpoint.clone();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let base_path = target.path().trim_end_matches('/').to_owned();
    let (path, query) = match path_and_query.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_and_query, None),
    };
    target.set_path(&format!("{base_path}{path}"));
    target.set_query(query);

    let body = rewrite(&proxy.rules, body);
    log_body(&proxy, Direction::Request, Payload::Bytes(&body));

    let mut upstream = proxy
        .client
        .request(parts.method.clone(), target)
        .body(body);
    for (name, value) in &parts.headers {
        if !is_hop_by_hop(name.as_str()) {
            upstream = upstream.header(name, value);
        }
    }

    let resp = match upstream.send().await {
        Ok(resp) => resp,
        Err(err) => {
            proxy.diag(&format!("<<< upstream error: {err:#}"));
            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from(format!("upstream error: {err}"))))?);
        }
    };

    let status = resp.status();
    let headers = resp.headers().clone();
    let resp_body = resp.bytes().await.context("reading upstream body")?;

    // A streamed response is logged as the one message its events describe.
    // If the body is not a recognizable message stream, fall back to bytes.
    let is_event_stream = headers
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"));
    let reconstructed = match is_event_stream {
        true => std::str::from_utf8(&resp_body)
            .ok()
            .and_then(stream::reconstruct),
        false => None,
    };
    // A transformed stream is re-emitted as a fresh event stream so the
    // client still receives what it asked for. Untouched bodies are
    // forwarded byte for byte.
    let (resp_body, message) = match reconstructed {
        Some(mut msg) => {
            let body = if transform::apply_rules(&proxy.rules, &mut msg) {
                Bytes::from(stream::emit(&msg))
            } else {
                resp_body
            };
            (body, Some(msg))
        }
        None => (rewrite(&proxy.rules, resp_body), None),
    };
    let payload = match &message {
        Some(msg) => Payload::Json(msg),
        None => Payload::Bytes(&resp_body),
    };
    log_body(
        &proxy,
        Direction::Response {
            ok: status.is_success(),
        },
        payload,
    );

    let mut out = Response::builder().status(status);
    for (name, value) in &headers {
        if !is_hop_by_hop(name.as_str()) {
            out = out.header(name, value);
        }
    }
    Ok(out.body(Full::new(resp_body))?)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Usage: llm-proxy [--raw] [config-path]
    let mut raw = false;
    let mut config_path: Option<String> = None;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--raw" => raw = true,
            flag if flag.starts_with("--") => bail!("unknown option {flag}"),
            _ if config_path.is_none() => config_path = Some(arg),
            _ => bail!("unexpected argument {arg}"),
        }
    }
    let config_path = config_path.unwrap_or_else(|| "config.toml".into());
    let text = tokio::fs::read_to_string(&config_path)
        .await
        .with_context(|| format!("reading {config_path}"))?;
    let config: Config = toml::from_str(&text).with_context(|| format!("parsing {config_path}"))?;

    let listen = Url::parse(&config.listen).context("parsing listen URL")?;
    if listen.scheme() != "http" {
        bail!("listen URL must use http, got {}", listen.scheme());
    }
    let host = listen.host_str().context("listen URL has no host")?;
    let port = listen.port().unwrap_or(80);
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .with_context(|| format!("listen host must be an IP address, got {host}"))?;

    let endpoint = Url::parse(&config.endpoint).context("parsing endpoint URL")?;
    match endpoint.scheme() {
        "http" | "https" => {}
        other => bail!("endpoint URL must use http or https, got {other}"),
    }

    let rules = transform::parse_rules(&config.regex).context("regex")?;

    let proxy = Arc::new(Proxy {
        client: reqwest::Client::new(),
        endpoint,
        rules,
        color: std::io::stdout().is_terminal(),
        raw,
    });

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    proxy.diag(&format!(
        "listening on http://{addr}, forwarding to {}",
        proxy.endpoint
    ));

    loop {
        let (stream, _) = listener.accept().await?;
        let proxy = proxy.clone();
        tokio::spawn(async move {
            let handler = proxy.clone();
            let service = service_fn(move |req| handle(handler.clone(), req));
            if let Err(err) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                proxy.diag(&format!("connection error: {err}"));
            }
        });
    }
}
