//! Reconstruction of a complete Messages API response from a buffered
//! server-sent event stream. Mirrors the accumulation the official SDKs do
//! for `get_final_message`: the result holds exactly the fields and text the
//! events delivered, in arrival order, with nothing added.

use serde_json::{Map, Value};

/// Splits an SSE body into the `data:` payload of each event. Multi-line
/// `data:` fields are joined with `\n` per the SSE spec; `event:`, `id:`,
/// comment and `retry:` lines are ignored because every payload carries its
/// own `type`.
fn events(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut data: Option<String> = None;
    for line in body.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            if let Some(d) = data.take() {
                out.push(d);
            }
            continue;
        }
        if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            match &mut data {
                Some(d) => {
                    d.push('\n');
                    d.push_str(rest);
                }
                None => data = Some(rest.to_owned()),
            }
        }
    }
    if let Some(d) = data {
        out.push(d);
    }
    out
}

/// Rebuilds the message from a buffered event stream.
///
/// Returns `None` when the body does not begin with a `message_start` event,
/// so the caller can fall back to logging the bytes verbatim. An `error`
/// event returns the error object in place of the message. A stream that
/// ends early yields whatever had been assembled so far.
pub fn reconstruct(body: &str) -> Option<Value> {
    let mut message: Option<Value> = None;
    // Accumulated `input_json_delta` fragments per content block index; a
    // block's `input` is only parseable once its fragments are complete.
    let mut partial_json: Vec<Option<String>> = Vec::new();

    for data in events(body) {
        let Ok(event) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let kind = event.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "message_start" => {
                let mut msg = event.get("message").cloned().unwrap_or(Value::Null);
                if msg.get("content").is_none()
                    && let Some(obj) = msg.as_object_mut()
                {
                    obj.insert("content".into(), Value::Array(Vec::new()));
                }
                message = Some(msg);
            }
            "error" => {
                return Some(event);
            }
            "ping" => {}
            _ => {
                let Some(msg) = message.as_mut() else {
                    continue;
                };
                apply(msg, kind, &event, &mut partial_json);
            }
        }
    }

    let mut msg = message?;
    // A truncated stream may leave tool inputs without a content_block_stop.
    for (index, buf) in partial_json.iter_mut().enumerate() {
        if let Some(buf) = buf.take() {
            finish_input(&mut msg, index, &buf);
        }
    }
    Some(msg)
}

fn apply(msg: &mut Value, kind: &str, event: &Value, partial_json: &mut Vec<Option<String>>) {
    let index = event.get("index").and_then(Value::as_u64).map(|i| i as usize);
    match kind {
        "content_block_start" => {
            let (Some(index), Some(block)) = (index, event.get("content_block")) else {
                return;
            };
            let content = content_array(msg);
            if content.len() <= index {
                content.resize(index + 1, Value::Null);
            }
            content[index] = block.clone();
        }
        "content_block_delta" => {
            let (Some(index), Some(delta)) = (index, event.get("delta")) else {
                return;
            };
            let delta_type = delta.get("type").and_then(Value::as_str).unwrap_or("");
            if delta_type == "input_json_delta" {
                if partial_json.len() <= index {
                    partial_json.resize(index + 1, None);
                }
                let fragment = delta
                    .get("partial_json")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                partial_json[index]
                    .get_or_insert_with(String::new)
                    .push_str(fragment);
                return;
            }
            let content = content_array(msg);
            let Some(block) = content.get_mut(index).and_then(Value::as_object_mut) else {
                return;
            };
            match delta_type {
                "text_delta" => append_str(block, "text", delta.get("text")),
                "thinking_delta" => append_str(block, "thinking", delta.get("thinking")),
                "signature_delta" => {
                    if let Some(sig) = delta.get("signature") {
                        block.insert("signature".into(), sig.clone());
                    }
                }
                "citations_delta" => {
                    if let Some(citation) = delta.get("citation")
                        && let Some(arr) = block
                            .entry("citations")
                            .or_insert_with(|| Value::Array(Vec::new()))
                            .as_array_mut()
                    {
                        arr.push(citation.clone());
                    }
                }
                // Unknown delta types carry no known merge rule; dropping
                // them is the only option that does not invent structure.
                _ => {}
            }
        }
        "content_block_stop" => {
            let Some(index) = index else { return };
            if let Some(buf) = partial_json.get_mut(index).and_then(Option::take) {
                finish_input(msg, index, &buf);
            }
        }
        "message_delta" => {
            // Top-level delta fields (stop_reason, stop_sequence, stop_details
            // ...) replace their counterparts; usage fields merge into the
            // existing usage object so input-token counts from message_start
            // survive.
            if let (Some(delta), Some(obj)) = (
                event.get("delta").and_then(Value::as_object),
                msg.as_object_mut(),
            ) {
                for (k, v) in delta {
                    obj.insert(k.clone(), v.clone());
                }
            }
            if let (Some(usage), Some(obj)) = (
                event.get("usage").and_then(Value::as_object),
                msg.as_object_mut(),
            ) {
                let target = obj
                    .entry("usage")
                    .or_insert_with(|| Value::Object(Map::new()));
                if let Some(target) = target.as_object_mut() {
                    for (k, v) in usage {
                        target.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        "message_stop" => {}
        _ => {}
    }
}

/// Serializes a complete message back into an event stream, for delivering a
/// transformed response to a client that asked for streaming. Each content
/// block becomes start, one delta, stop; the client receives the same
/// content it would have reconstructed, in one burst.
pub fn emit(msg: &Value) -> Vec<u8> {
    fn event(out: &mut Vec<u8>, kind: &str, payload: &Value) {
        out.extend_from_slice(format!("event: {kind}\ndata: ").as_bytes());
        out.extend_from_slice(&serde_json::to_vec(payload).expect("serializing a Value"));
        out.extend_from_slice(b"\n\n");
    }
    let mut out = Vec::new();
    let empty = Vec::new();
    let blocks = msg
        .get("content")
        .and_then(Value::as_array)
        .unwrap_or(&empty);

    // message_start carries the message shell: everything except content and
    // the stop fields, which arrive in message_delta.
    let mut shell = msg.clone();
    if let Some(obj) = shell.as_object_mut() {
        obj.insert("content".into(), Value::Array(Vec::new()));
        for key in ["stop_reason", "stop_sequence", "stop_details"] {
            if obj.contains_key(key) {
                obj.insert(key.into(), Value::Null);
            }
        }
    }
    event(
        &mut out,
        "message_start",
        &serde_json::json!({"type": "message_start", "message": shell}),
    );

    for (index, block) in blocks.iter().enumerate() {
        let kind = block.get("type").and_then(Value::as_str).unwrap_or("");
        let mut start = block.clone();
        let mut deltas: Vec<Value> = Vec::new();
        if let Some(obj) = start.as_object_mut() {
            match kind {
                "text" => {
                    if let Some(text) = obj.insert("text".into(), Value::String(String::new())) {
                        deltas.push(serde_json::json!({"type": "text_delta", "text": text}));
                    }
                }
                "thinking" => {
                    if let Some(t) = obj.insert("thinking".into(), Value::String(String::new())) {
                        deltas.push(serde_json::json!({"type": "thinking_delta", "thinking": t}));
                    }
                    if let Some(sig) = obj.remove("signature") {
                        deltas.push(
                            serde_json::json!({"type": "signature_delta", "signature": sig}),
                        );
                    }
                }
                "tool_use" | "server_tool_use" | "mcp_tool_use" => {
                    if let Some(input) = obj.insert(
                        "input".into(),
                        Value::Object(serde_json::Map::new()),
                    ) {
                        let json = serde_json::to_string(&input).expect("serializing a Value");
                        deltas.push(
                            serde_json::json!({"type": "input_json_delta", "partial_json": json}),
                        );
                    }
                }
                // Other block types are sent whole in content_block_start.
                _ => {}
            }
        }
        event(
            &mut out,
            "content_block_start",
            &serde_json::json!({"type": "content_block_start", "index": index, "content_block": start}),
        );
        for delta in deltas {
            event(
                &mut out,
                "content_block_delta",
                &serde_json::json!({"type": "content_block_delta", "index": index, "delta": delta}),
            );
        }
        event(
            &mut out,
            "content_block_stop",
            &serde_json::json!({"type": "content_block_stop", "index": index}),
        );
    }

    let mut delta = serde_json::Map::new();
    for key in ["stop_reason", "stop_sequence", "stop_details"] {
        if let Some(v) = msg.get(key) {
            delta.insert(key.into(), v.clone());
        }
    }
    let usage = msg.get("usage").cloned().unwrap_or(Value::Null);
    event(
        &mut out,
        "message_delta",
        &serde_json::json!({"type": "message_delta", "delta": delta, "usage": usage}),
    );
    event(&mut out, "message_stop", &serde_json::json!({"type": "message_stop"}));
    out
}

fn content_array(msg: &mut Value) -> &mut Vec<Value> {
    msg.as_object_mut()
        .expect("message is an object")
        .entry("content")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .expect("content is an array")
}

fn append_str(block: &mut Map<String, Value>, key: &str, piece: Option<&Value>) {
    let Some(piece) = piece.and_then(Value::as_str) else {
        return;
    };
    match block.get_mut(key) {
        Some(Value::String(s)) => s.push_str(piece),
        _ => {
            block.insert(key.into(), Value::String(piece.to_owned()));
        }
    }
}

/// Parses accumulated `input_json_delta` fragments into the block's `input`.
/// An empty accumulation means a tool call with no arguments (`{}`). Text
/// that does not parse (a truncated stream) is kept under `partial_json` so
/// nothing that arrived is lost.
fn finish_input(msg: &mut Value, index: usize, buf: &str) {
    let content = content_array(msg);
    let Some(block) = content.get_mut(index).and_then(Value::as_object_mut) else {
        return;
    };
    if buf.trim().is_empty() {
        block.insert("input".into(), Value::Object(Map::new()));
        return;
    }
    match serde_json::from_str::<Value>(buf) {
        Ok(v) => {
            block.insert("input".into(), v);
        }
        Err(_) => {
            block.insert("partial_json".into(), Value::String(buf.to_owned()));
        }
    }
}
