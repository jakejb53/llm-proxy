//! sed-style rewrite rules applied to the string values of a JSON body.
//!
//! Rule syntax: `s<d>PATTERN<d>REPLACEMENT[<d>[FLAGS]]` where `<d>` is any
//! single delimiter character. The trailing delimiter and flags are optional.
//! A backslash before the delimiter inside PATTERN or REPLACEMENT makes it
//! literal. Full rules are documented in config.toml next to the examples.

use anyhow::{Context, Result, bail};
use regex::Regex;
use serde_json::Value;

pub struct Rule {
    regex: Regex,
    replacement: String,
    global: bool,
}

impl Rule {
    pub fn parse(spec: &str) -> Result<Rule> {
        let mut chars = spec.chars();
        if chars.next() != Some('s') {
            bail!("rule must start with 's'");
        }
        let delim = chars.next().context("rule has no delimiter after 's'")?;
        if delim == '\\' || delim.is_alphanumeric() || delim.is_whitespace() {
            bail!("delimiter {delim:?} is not allowed");
        }
        let rest: String = chars.collect();

        // Split on unescaped delimiters. `\<d>` becomes a literal `<d>`; every
        // other backslash sequence passes through untouched so regex escapes
        // like `\d` survive.
        let mut fields: Vec<String> = vec![String::new()];
        let mut it = rest.chars().peekable();
        while let Some(c) = it.next() {
            if c == '\\' && it.peek() == Some(&delim) {
                fields.last_mut().unwrap().push(delim);
                it.next();
            } else if c == delim {
                fields.push(String::new());
            } else {
                fields.last_mut().unwrap().push(c);
            }
        }
        let (pattern, replacement, flags) = match fields.len() {
            // s/pat/rep  (no trailing delimiter)
            2 => (&fields[0], &fields[1], ""),
            // s/pat/rep/flags
            3 => (&fields[0], &fields[1], fields[2].as_str()),
            1 => bail!("rule has no replacement"),
            _ => bail!("rule has too many delimiters; escape literal {delim:?} as \\{delim}"),
        };

        let mut global = false;
        let mut inline = String::new();
        for f in flags.chars() {
            match f {
                'g' => global = true,
                'i' | 's' | 'm' => inline.push(f),
                other => bail!("unknown flag {other:?}"),
            }
        }
        let full = if inline.is_empty() {
            pattern.clone()
        } else {
            format!("(?{inline}){pattern}")
        };
        let regex = Regex::new(&full).with_context(|| format!("invalid pattern {pattern:?}"))?;
        Ok(Rule {
            regex,
            replacement: replacement.clone(),
            global,
        })
    }

    fn apply(&self, text: &str) -> Option<String> {
        if !self.regex.is_match(text) {
            return None;
        }
        let out = if self.global {
            self.regex.replace_all(text, self.replacement.as_str())
        } else {
            self.regex.replace(text, self.replacement.as_str())
        };
        Some(out.into_owned())
    }
}

/// Parses every rule in `specs`, naming the offending rule on failure.
pub fn parse_rules(specs: &[String]) -> Result<Vec<Rule>> {
    specs
        .iter()
        .map(|s| Rule::parse(s).with_context(|| format!("rule {s:?}")))
        .collect()
}

/// Runs every rule, in order, over every string value in `value`. Object
/// keys are never touched. Returns whether anything changed so callers can
/// forward untouched bodies byte for byte.
pub fn apply_rules(rules: &[Rule], value: &mut Value) -> bool {
    match value {
        Value::String(s) => {
            let mut changed = false;
            for rule in rules {
                if let Some(next) = rule.apply(s) {
                    *s = next;
                    changed = true;
                }
            }
            changed
        }
        // Every child must be visited even after one changes, so no
        // short-circuiting combinator here.
        Value::Array(items) => {
            let mut changed = false;
            for v in items {
                changed |= apply_rules(rules, v);
            }
            changed
        }
        Value::Object(fields) => {
            let mut changed = false;
            for v in fields.values_mut() {
                changed |= apply_rules(rules, v);
            }
            changed
        }
        _ => false,
    }
}
