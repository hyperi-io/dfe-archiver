// Project:   dfe-archiver
// File:      crates/core/src/routing/mod.rs
// Purpose:   Message routing by topic or expression
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::config::RoutingConfig;
use crate::types::KafkaMessage;
use crate::{Error, Result};
use compact_str::CompactString;
use sonic_rs::JsonValueTrait;

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
            "topic" => Ok(self.route_by_topic(message)),
            "expression" => self.route_by_expression(message),
            _ => Ok(self.route_by_topic(message)),
        }
    }

    /// Route by Kafka topic name
    fn route_by_topic(&self, message: &KafkaMessage) -> CompactString {
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
            segments.push(value.unwrap_or_else(|| self.config.default_segment.clone()));
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
        } else if let Some(n) = current.as_f64() {
            Some(n.to_string())
        } else {
            None
        }
    } else if let Some(b) = current.as_bool() {
        Some(b.to_string())
    } else {
        None
    }
}

#[cfg(test)]
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
}
