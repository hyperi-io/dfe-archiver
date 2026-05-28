// Project:   dfe-archiver
// File:      crates/core/src/routing/mod.rs
// Purpose:   Message routing by topic or expression
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::config::RoutingConfig;
use crate::types::KafkaMessage;
use crate::{Error, Result};
use compact_str::CompactString;
use sonic_rs::JsonValueTrait;
use tracing::trace;

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

    /// Route message to destination path
    pub fn route(&self, message: &KafkaMessage) -> Result<CompactString> {
        match self.config.mode.as_str() {
            "expression" => self.route_by_expression(message),
            _ => Ok(Self::route_by_topic(message)),
        }
    }

    /// Route by Kafka topic name
    fn route_by_topic(message: &KafkaMessage) -> CompactString {
        message.topic.clone()
    }

    /// Route by JSON field expressions
    fn route_by_expression(&self, message: &KafkaMessage) -> Result<CompactString> {
        let json: sonic_rs::Value = sonic_rs::from_slice(&message.payload)
            .map_err(|e| Error::Routing(format!("invalid JSON: {e}")))?;

        let mut segments = Vec::with_capacity(self.config.expression_fields.len() + 1);
        segments.push(message.topic.to_string());

        for field_path in &self.config.expression_fields {
            let value = extract_field(&json, field_path);
            let resolved = value.unwrap_or_else(|| self.config.default_segment.clone());
            trace!(field = %field_path, value = %resolved, "Expression routing field");
            segments.push(resolved);
        }

        Ok(CompactString::from(segments.join("/")))
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

        let dest = router.route(&message).expect("route");
        assert_eq!(dest.as_str(), "events");
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

        let dest = router.route(&message).expect("route");
        assert_eq!(dest.as_str(), "events/acme/login");
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

        let dest = router.route(&message).expect("route");
        assert_eq!(dest.as_str(), "events/acme/unknown");
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

        let dest = router.route(&message).expect("route");
        assert_eq!(dest.as_str(), "events/security");
    }

    #[test]
    fn test_route_empty_payload() {
        let config = RoutingConfig {
            mode: "topic".to_string(),
            ..Default::default()
        };
        let router = Router::new(config);
        let msg = KafkaMessage::for_test(vec![], "events", 0, 0);
        let dest = router.route(&msg).expect("route empty payload");
        assert_eq!(dest.as_str(), "events");
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
        let dest = router.route(&msg).expect("route deep");
        assert_eq!(dest.as_str(), "events/deep");
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
        let dest = router.route(&msg).expect("route deep missing");
        assert_eq!(dest.as_str(), "events/nope");
    }
}
