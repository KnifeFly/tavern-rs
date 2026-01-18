use std::io::{self, Read};

const MARKER: [&str; 17] = [
    "Client-Ip",
    "Domain",
    "Content-Type",
    "RequestTime",
    "",
    "Method",
    "Status",
    "SentBytes(header+body)",
    "Referer",
    "UserAgent",
    "ResponseTime(ms)",
    "BodySize",
    "ContentLength",
    "Range",
    "X-Forwarded-For",
    "CacheStatus",
    "RequestID",
];

fn main() {
    let mut buf = String::new();
    if io::stdin().read_to_string(&mut buf).is_err() {
        return;
    }

    for line in buf.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        let mut out = String::new();
        for (i, field) in fields.iter().enumerate() {
            if i >= MARKER.len() {
                break;
            }
            let mark = MARKER[i];
            if mark.is_empty() {
                continue;
            }
            out.push('(');
            out.push_str(&i.to_string());
            out.push(')');
            out.push_str(mark);
            out.push_str(": ");
            out.push_str(field);
            if i + 1 < MARKER.len() && MARKER[i + 1].is_empty() {
                if let Some(next) = fields.get(i + 1) {
                    out.push_str(next);
                }
            }
            out.push('\n');
        }
        println!("{out}");
    }
}
