// Project:   dfe-archiver
// File:      crates/core/src/routing/mod.rs
// Purpose:   Message routing by topic or expression
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

pub mod depth;

use crate::config::RoutingConfig;
use crate::types::KafkaMessage;
use crate::{Error, Result};
use compact_str::CompactString;
use depth::{MAX_PARSE_DEPTH, json_depth_within};
use sonic_rs::JsonValueTrait;
use tracing::trace;

/// Where a message is archived, and which configured expression fields the
/// payload did not carry.
///
/// This crate owns no metrics, so the router reports the fallback and the
/// archiver crate counts it -- otherwise a misconfigured field name is
/// indistinguishable from a tenant genuinely called `unknown`.
#[derive(Debug, Clone)]
pub struct Routed {
    /// Destination path segment the writer archives under.
    pub destination: CompactString,

    /// Indices into `RoutingConfig::expression_fields` that fell through to
    /// `default_segment`. Empty on the healthy path, so a correctly configured
    /// deployment allocates nothing here.
    pub fallback_fields: Vec<usize>,
}

/// Router for determining archive destination
pub struct Router {
    config: RoutingConfig,
}

impl Router {
    /// Create new router
    #[must_use]
    pub fn new(config: RoutingConfig) -> Self {
        Self { config }
    }

    /// The expression fields this router resolves, in the order
    /// `Routed::fallback_fields` indexes them.
    #[must_use]
    pub fn expression_fields(&self) -> &[String] {
        &self.config.expression_fields
    }

    /// Route message to destination path
    ///
    /// # Errors
    ///
    /// In expression mode, [`Error::TooDeep`] for a payload nested past
    /// [`MAX_PARSE_DEPTH`], which is never parsed, and [`Error::Routing`] for
    /// one that is not JSON.
    pub fn route(&self, message: &KafkaMessage) -> Result<Routed> {
        match self.config.mode.as_str() {
            "expression" => self.route_by_expression(message),
            _ => Ok(Routed {
                destination: Self::route_by_topic(message),
                fallback_fields: Vec::new(),
            }),
        }
    }

    /// Route by Kafka topic name
    fn route_by_topic(message: &KafkaMessage) -> CompactString {
        message.topic.clone()
    }

    /// Route by JSON field expressions
    fn route_by_expression(&self, message: &KafkaMessage) -> Result<Routed> {
        // sonic-rs recurses per level with no limit, so a deep record would overflow the stack.
        if !json_depth_within(&message.payload, MAX_PARSE_DEPTH) {
            return Err(Error::TooDeep {
                max: MAX_PARSE_DEPTH,
            });
        }
        let json: sonic_rs::Value = sonic_rs::from_slice(&message.payload)
            .map_err(|e| Error::Routing(format!("invalid JSON: {e}")))?;

        let mut segments = Vec::with_capacity(self.config.expression_fields.len() + 1);
        segments.push(message.topic.to_string());
        let mut fallback_fields = Vec::new();

        for (index, field_path) in self.config.expression_fields.iter().enumerate() {
            let resolved = if let Some(value) = extract_field(&json, field_path) {
                value
            } else {
                fallback_fields.push(index);
                self.config.default_segment.clone()
            };
            trace!(field = %field_path, value = %resolved, "Expression routing field");
            segments.push(resolved);
        }

        Ok(Routed {
            destination: CompactString::from(segments.join("/")),
            fallback_fields,
        })
    }
}

/// Extract field value from JSON using dot notation
fn extract_field(json: &sonic_rs::Value, path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = json;

    for part in parts {
        current = current.get(part)?;
    }

    if let Some(s) = current.as_str() {
        Some(s.to_string())
    } else if current.is_number() {
        if let Some(n) = current.as_i64() {
            Some(n.to_string())
        } else if let Some(n) = current.as_u64() {
            Some(n.to_string())
        } else {
            current.as_f64().map(|n| n.to_string())
        }
    } else {
        current.as_bool().map(|b| b.to_string())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn make_message(topic: &str, payload: &str) -> KafkaMessage {
        KafkaMessage::for_test(payload.as_bytes().to_vec(), topic, 0, 0)
    }

    #[test]
    fn test_route_by_topic() {
        let config = RoutingConfig {
            mode: "topic".to_string(),
            ..Default::default()
        };

        let router = Router::new(config);
        let message = make_message("events", r#"{"foo": "bar"}"#);

        let outcome = router.route(&message).expect("route");
        assert_eq!(outcome.destination.as_str(), "events");
        assert!(outcome.fallback_fields.is_empty());
    }

    #[test]
    fn test_route_by_expression() {
        let config = RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: vec!["org_id".to_string(), "event_type".to_string()],
            default_segment: "unknown".to_string(),
        };

        let router = Router::new(config);
        let message = make_message(
            "events",
            r#"{"org_id": "acme", "event_type": "login", "data": {}}"#,
        );

        let outcome = router.route(&message).expect("route");
        assert_eq!(outcome.destination.as_str(), "events/acme/login");
        assert!(
            outcome.fallback_fields.is_empty(),
            "a payload carrying every field took no fallback"
        );
    }

    #[test]
    fn test_route_by_expression_missing_field() {
        let config = RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: vec!["org_id".to_string(), "missing".to_string()],
            default_segment: "unknown".to_string(),
        };

        let router = Router::new(config);
        let message = make_message("events", r#"{"org_id": "acme"}"#);

        let outcome = router.route(&message).expect("route");
        assert_eq!(outcome.destination.as_str(), "events/acme/unknown");
        assert_eq!(
            outcome.fallback_fields,
            vec![1],
            "only the missing field reports a fallback"
        );
    }

    #[test]
    fn test_route_by_nested_expression() {
        let config = RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: vec!["tags.category".to_string()],
            default_segment: "unknown".to_string(),
        };

        let router = Router::new(config);
        let message = make_message("events", r#"{"tags": {"category": "security"}}"#);

        let outcome = router.route(&message).expect("route");
        assert_eq!(outcome.destination.as_str(), "events/security");
    }

    #[test]
    fn test_route_empty_payload() {
        let config = RoutingConfig {
            mode: "topic".to_string(),
            ..Default::default()
        };
        let router = Router::new(config);
        let msg = KafkaMessage::for_test(vec![], "events", 0, 0);
        let outcome = router.route(&msg).expect("route empty payload");
        assert_eq!(outcome.destination.as_str(), "events");
    }

    #[test]
    fn test_route_non_json_with_expression_returns_error() {
        let config = RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: vec!["org_id".to_string()],
            default_segment: "fallback".to_string(),
        };
        let router = Router::new(config);
        let msg = make_message("events", "this is not json");
        let result = router.route(&msg);
        assert!(result.is_err(), "non-JSON payload should return Err");
    }

    #[test]
    fn test_route_deeply_nested_field() {
        let config = RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: vec!["a.b.c.d".to_string()],
            default_segment: "missing".to_string(),
        };
        let router = Router::new(config);
        let msg = make_message("events", r#"{"a":{"b":{"c":{"d":"deep"}}}}"#);
        let outcome = router.route(&msg).expect("route deep");
        assert_eq!(outcome.destination.as_str(), "events/deep");
    }

    #[test]
    fn test_route_deeply_nested_missing() {
        let config = RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: vec!["a.b.c.d.e.f".to_string()],
            default_segment: "nope".to_string(),
        };
        let router = Router::new(config);
        let msg = make_message("events", r#"{"a":{"b":"leaf"}}"#);
        let outcome = router.route(&msg).expect("route deep missing");
        assert_eq!(outcome.destination.as_str(), "events/nope");
        assert_eq!(outcome.fallback_fields, vec![0]);
    }

    /// Topic routing resolves no fields, so it can never report a fallback --
    /// the counter must stay specific to a misconfigured expression field.
    #[test]
    fn test_topic_routing_reports_no_fallback() {
        let config = RoutingConfig {
            mode: "topic".to_string(),
            expression_fields: vec!["org_id".to_string()],
            default_segment: "unknown".to_string(),
        };
        let router = Router::new(config);
        let msg = make_message("events", r#"{"nothing": "here"}"#);

        let outcome = router.route(&msg).expect("route by topic");
        assert!(outcome.fallback_fields.is_empty());
    }

    /// Every configured field missing is the misconfiguration this reports:
    /// a field name that no source in the deployment carries.
    #[test]
    fn test_every_missing_field_reports_a_fallback() {
        let config = RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: vec!["org_id".to_string(), "tenant_id".to_string()],
            default_segment: "unknown".to_string(),
        };
        let router = Router::new(config);
        let msg = make_message("events", r#"{"customer": "acme"}"#);

        let outcome = router.route(&msg).expect("route");
        assert_eq!(outcome.destination.as_str(), "events/unknown/unknown");
        assert_eq!(outcome.fallback_fields, vec![0, 1]);
    }

    // ---- nesting depth ----

    /// The stack a Tokio or rayon worker thread gets by default.
    const WORKER_STACK: usize = 2 * 1024 * 1024;

    /// A stack that holds 64 levels of `sonic_rs::Value` parsing, whose frames
    /// are far larger in a debug build than in release.
    const BOUND_STACK: usize = if cfg!(debug_assertions) {
        16 * 1024 * 1024
    } else {
        WORKER_STACK
    };

    /// Run `test` on a thread with `stack` bytes of stack, and fail unless it returns.
    fn on_stack(stack: usize, test: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(stack)
            .spawn(test)
            .expect("spawn the routing thread")
            .join()
            .expect("the routing thread must return");
    }

    fn nested_array(depth: usize) -> String {
        format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
    }

    fn nested_object(depth: usize) -> String {
        format!("{}1{}", "{\"a\":".repeat(depth), "}".repeat(depth))
    }

    fn router_in(mode: &str) -> Router {
        Router::new(RoutingConfig {
            mode: mode.to_string(),
            expression_fields: vec!["org_id".to_string()],
            default_segment: "unknown".to_string(),
        })
    }

    #[test]
    fn a_deeply_nested_record_is_refused_without_parsing_it() {
        on_stack(WORKER_STACK, || {
            let expression = router_in("expression");
            let topic = router_in("topic");
            for depth in [20_000, 100_000] {
                let deep = [
                    nested_array(depth),
                    nested_object(depth),
                    // A deep sibling ahead of the routing field.
                    format!(r#"{{"sibling":{},"org_id":"acme"}}"#, nested_array(depth)),
                ];
                for payload in deep {
                    let message = make_message("events", &payload);
                    let refused = expression
                        .route(&message)
                        .expect_err("a record past the bound must be refused");
                    assert!(
                        matches!(refused, Error::TooDeep { max: 64 }),
                        "depth {depth}: {refused}"
                    );
                    assert_eq!(
                        refused.to_string(),
                        "payload nesting exceeds the maximum parse depth of 64"
                    );
                    // Topic routing never parses, so it has nothing to refuse.
                    let routed = topic.route(&message).expect("route by topic");
                    assert_eq!(routed.destination.as_str(), "events");
                }
            }
        });
    }

    #[test]
    fn the_bound_is_64_levels() {
        on_stack(BOUND_STACK, || {
            let router = router_in("expression");
            // The object around the routing field is the first level.
            let at = format!(r#"{{"org_id":"acme","n":{}}}"#, nested_array(63));
            let over = format!(r#"{{"org_id":"acme","n":{}}}"#, nested_array(64));

            let outcome = router
                .route(&make_message("events", &at))
                .expect("64 levels are parsed and routed");
            assert_eq!(outcome.destination.as_str(), "events/acme");
            let refused = router
                .route(&make_message("events", &over))
                .expect_err("65 levels are refused");
            assert!(matches!(refused, Error::TooDeep { max: 64 }), "{refused}");
        });
    }
}
