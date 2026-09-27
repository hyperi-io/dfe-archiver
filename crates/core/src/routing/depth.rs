// Project:   dfe-archiver
// File:      crates/core/src/routing/depth.rs
// Purpose:   Nesting-depth pre-check for untrusted JSON
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Nesting-depth pre-check for untrusted JSON.
//!
//! sonic-rs builds a `sonic_rs::Value` by recursing once per nesting level with
//! no depth limit, so a record nested some thousands of levels deep exhausts a
//! 2 MiB worker stack and aborts the process, and at-least-once delivery hands
//! the same record back after the restart. Expression routing measures every
//! record here, iteratively, before it parses one.

/// Deepest nesting a record may reach, the bound scalo's parse path uses.
pub const MAX_PARSE_DEPTH: usize = 64;

// SHORTCUT: app-local copy of scalo's json_depth_within, until scalo exposes it
/// `true` if the JSON payload nests no deeper than `max`.
///
/// One forward pass counting `{` and `[` outside strings, honouring `\`
/// escapes. Not a validator: on malformed input the parser stops at the first
/// bad token, which is no deeper than this pass has already counted.
#[must_use]
pub fn json_depth_within(payload: &[u8], max: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &b in payload {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(open: &str, close: &str, depth: usize) -> Vec<u8> {
        let mut payload = open.repeat(depth).into_bytes();
        payload.extend_from_slice(b"1");
        payload.extend_from_slice(close.repeat(depth).as_bytes());
        payload
    }

    #[test]
    fn flat_and_shallow_pass() {
        assert!(json_depth_within(br"{}", MAX_PARSE_DEPTH));
        assert!(json_depth_within(
            br#"{"a":1,"b":[1,2,3]}"#,
            MAX_PARSE_DEPTH
        ));
        assert!(json_depth_within(
            br#"{"a":{"b":{"c":1}}}"#,
            MAX_PARSE_DEPTH
        ));
        assert!(json_depth_within(b"", MAX_PARSE_DEPTH));
    }

    #[test]
    fn exactly_at_the_bound_passes_and_one_over_fails() {
        assert!(json_depth_within(&nested("[", "]", 3), 3));
        assert!(!json_depth_within(&nested("[", "]", 4), 3));
        assert!(json_depth_within(
            &nested("{\"a\":", "}", MAX_PARSE_DEPTH),
            MAX_PARSE_DEPTH
        ));
        assert!(!json_depth_within(
            &nested("{\"a\":", "}", MAX_PARSE_DEPTH + 1),
            MAX_PARSE_DEPTH
        ));
    }

    #[test]
    fn sibling_containers_do_not_add_up() {
        let wide = format!("[{}]", vec!["[[1]]"; 1000].join(","));
        assert!(json_depth_within(wide.as_bytes(), 3));
    }

    #[test]
    fn brackets_inside_strings_do_not_count() {
        assert!(json_depth_within(br#"{"k":"{{{{{{{{[[[[["}"#, 2));
    }

    #[test]
    fn an_escaped_quote_keeps_the_string_open() {
        assert!(json_depth_within(br#"{"k":"a\"{{{{{"}"#, 2));
    }

    #[test]
    fn an_escaped_backslash_closes_the_string() {
        // `\\` is one literal backslash, so the quote after it ends the string
        // and the brackets that follow are structure.
        assert!(!json_depth_within(br#"["\\"[[[1]]]]"#, 3));
    }

    #[test]
    fn pathological_depth_is_refused() {
        for depth in [5_000, 20_000, 100_000] {
            assert!(!json_depth_within(
                &nested("[", "]", depth),
                MAX_PARSE_DEPTH
            ));
            assert!(!json_depth_within(
                &nested("{\"a\":", "}", depth),
                MAX_PARSE_DEPTH
            ));
        }
    }
}
