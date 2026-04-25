use std::collections::HashMap;

use tavern::config::Bootstrap;
use tavern::proxy::{BalancePolicy, Node, ReverseProxy};

#[test]
fn parses_string_and_weighted_upstream_addresses() {
    let raw = r#"
server:
  addr: "127.0.0.1:0"
upstream:
  address:
    - "http://origin-a.example.com"
    - address: "https://origin-b.example.com"
      weight: 3
"#;
    let cfg: Bootstrap = serde_yaml::from_str(raw).expect("config");

    cfg.validate().expect("valid config");
    assert_eq!(
        cfg.upstream.address[0].address(),
        "http://origin-a.example.com"
    );
    assert_eq!(cfg.upstream.address[0].weight(), 1);
    assert_eq!(
        cfg.upstream.address[1].address(),
        "https://origin-b.example.com"
    );
    assert_eq!(cfg.upstream.address[1].weight(), 3);
    assert_eq!(
        cfg.upstream.address_strings(),
        vec![
            "http://origin-a.example.com".to_string(),
            "https://origin-b.example.com".to_string()
        ]
    );
}

#[test]
fn rejects_zero_upstream_weight() {
    let raw = r#"
server:
  addr: "127.0.0.1:0"
upstream:
  address:
    - address: "http://origin.example.com"
      weight: 0
"#;
    let cfg: Bootstrap = serde_yaml::from_str(raw).expect("config");

    let err = cfg.validate().expect_err("zero weight must fail");
    assert!(err.to_string().contains("weight 0"));
}

#[test]
fn weighted_round_robin_uses_node_weights() {
    let proxy = ReverseProxy::new(
        vec![
            Node::new("http", "origin-a.example.com", 1),
            Node::new("http", "origin-b.example.com", 3),
        ],
        BalancePolicy::WeightedRoundRobin,
    );
    let mut counts = HashMap::new();

    for _ in 0..8 {
        let node = proxy.next_node().expect("node");
        *counts.entry(node.address).or_insert(0usize) += 1;
    }

    assert_eq!(counts.get("origin-a.example.com"), Some(&2));
    assert_eq!(counts.get("origin-b.example.com"), Some(&6));
}
