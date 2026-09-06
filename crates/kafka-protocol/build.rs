use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

fn strip_json_comments(content: &str) -> String {
    let mut result = String::with_capacity(content.len());
    let mut in_string = false;
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.next() {
        if in_string {
            result.push(c);
            if c == '"' {
                in_string = false;
            }
            continue;
        }

        if c == '"' {
            in_string = true;
            result.push(c);
            continue;
        }

        if c == '/' {
            if let Some(&next) = chars.peek() {
                if next == '/' {
                    for ch in chars.by_ref() {
                        if ch == '\n' {
                            result.push('\n');
                            break;
                        }
                    }
                    continue;
                } else if next == '*' {
                    chars.next();
                    while let Some(ch) = chars.next() {
                        if ch == '*' {
                            if let Some(&n) = chars.peek() {
                                if n == '/' {
                                    chars.next();
                                    break;
                                }
                            }
                        }
                    }
                    continue;
                }
            }
            result.push(c);
            continue;
        }

        result.push(c);
    }

    result
}

fn parse_version_range(s: &str) -> (i16, i16) {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() == 2 {
        let min = parts[0].trim().parse::<i16>().unwrap_or(0);
        let max = parts[1].trim().parse::<i16>().unwrap_or(0);
        (min, max)
    } else {
        let ver = s.trim().parse::<i16>().unwrap_or(0);
        (ver, ver)
    }
}

/// Parse "flexibleVersions" field to get the minimum flexible version.
/// - "3+" → Some(3)
/// - "0+" → Some(0)
/// - "2-4" → Some(2) (only the start matters for our threshold check)
/// - "" or "null" → None
fn parse_flexible_min(s: &str) -> Option<i16> {
    if s.is_empty() || s == "null" {
        return None;
    }
    if let Some(ver) = s.strip_suffix('+') {
        return ver.trim().parse::<i16>().ok();
    }
    if s.contains('-') {
        let parts: Vec<&str> = s.split('-').collect();
        return parts[0].trim().parse::<i16>().ok();
    }
    None
}

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = PathBuf::from(&manifest_dir).join("../../");
    let message_dir = workspace_root.join("clients/src/main/resources/common/message/");

    println!("cargo:rerun-if-changed={}", message_dir.display());
    println!("cargo:rerun-if-changed=build.rs");

    let mut entries: BTreeMap<i16, (String, i16, i16, Option<i16>)> = BTreeMap::new();

    if let Ok(paths) = fs::read_dir(&message_dir) {
        for path in paths.flatten() {
            let file_name = path.file_name().to_string_lossy().to_string();
            if !file_name.ends_with("Request.json") {
                continue;
            }

            let content = match fs::read_to_string(path.path()) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let cleaned = strip_json_comments(&content);

            let json: Value = match serde_json::from_str(&cleaned) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let api_key = json["apiKey"].as_i64().map(|v| v as i16);
            let valid_versions = json["validVersions"].as_str().unwrap_or("0");
            let flexible_versions = json["flexibleVersions"].as_str().unwrap_or("");

            if let Some(key) = api_key {
                let full_name = json["name"].as_str().unwrap_or("");
                let display_name = full_name.strip_suffix("Request").unwrap_or(full_name);
                let (min_ver, max_ver) = parse_version_range(valid_versions);
                let flex_min = parse_flexible_min(flexible_versions);
                entries.insert(key, (display_name.to_string(), min_ver, max_ver, flex_min));
            }
        }
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    fs::create_dir_all(&out_dir).unwrap();
    let dest_path = out_dir.join("gen_api_key.rs");
    let mut f = fs::File::create(&dest_path).unwrap();

    writeln!(f, "// Auto-generated: do not edit.").unwrap();
    writeln!(f, "// Source: clients/src/main/resources/common/message/*Request.json").unwrap();
    writeln!(f).unwrap();
    writeln!(f, "use std::fmt;").unwrap();
    writeln!(f).unwrap();
    writeln!(f, "/// API key for Kafka protocol requests.").unwrap();
    writeln!(f, "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]").unwrap();
    writeln!(f, "pub enum ApiKey {{").unwrap();

    for (_, (name, _, _, _)) in &entries {
        writeln!(f, "    {},", name).unwrap();
    }

    writeln!(f, "    /// Unknown API key (for unrecognized values).").unwrap();
    writeln!(f, "    Unknown(i16),").unwrap();
    writeln!(f, "}}").unwrap();
    writeln!(f).unwrap();

    // from_id
    writeln!(f, "impl ApiKey {{").unwrap();
    writeln!(f, "    /// Convert an integer API key to the corresponding variant.").unwrap();
    writeln!(f, "    pub fn from_id(id: i16) -> Self {{").unwrap();
    writeln!(f, "        match id {{").unwrap();
    for (key, (name, _, _, _)) in &entries {
        writeln!(f, "            {key} => ApiKey::{name},").unwrap();
    }
    writeln!(f, "            other => ApiKey::Unknown(other),").unwrap();
    writeln!(f, "        }}").unwrap();
    writeln!(f, "    }}").unwrap();
    writeln!(f).unwrap();

    // to_id / id
    writeln!(f, "    /// Get the raw integer ID of this API key.").unwrap();
    writeln!(f, "    pub fn id(&self) -> i16 {{").unwrap();
    writeln!(f, "        match self {{").unwrap();
    for (key, (name, _, _, _)) in &entries {
        writeln!(f, "            ApiKey::{name} => {key},").unwrap();
    }
    writeln!(f, "            ApiKey::Unknown(id) => *id,").unwrap();
    writeln!(f, "        }}").unwrap();
    writeln!(f, "    }}").unwrap();
    writeln!(f).unwrap();

    // name
    writeln!(f, "    /// Human-readable name used in the wire protocol.").unwrap();
    writeln!(f, "    pub fn name(&self) -> &'static str {{").unwrap();
    writeln!(f, "        match self {{").unwrap();
    for (_, (name, _, _, _)) in &entries {
        writeln!(f, "            ApiKey::{name} => \"{name}\",").unwrap();
    }
    writeln!(f, "            ApiKey::Unknown(_) => \"Unknown\",").unwrap();
    writeln!(f, "        }}").unwrap();
    writeln!(f, "    }}").unwrap();
    writeln!(f).unwrap();

    // max_version
    writeln!(f, "    /// Highest supported version for this API key.").unwrap();
    writeln!(f, "    pub fn max_version(&self) -> i16 {{").unwrap();
    writeln!(f, "        match self {{").unwrap();
    for (_, (name, _, max_ver, _)) in &entries {
        writeln!(f, "            ApiKey::{name} => {max_ver},").unwrap();
    }
    writeln!(f, "            ApiKey::Unknown(_) => 0,").unwrap();
    writeln!(f, "        }}").unwrap();
    writeln!(f, "    }}").unwrap();
    writeln!(f).unwrap();

    // min_version
    writeln!(f, "    /// Lowest supported version for this API key.").unwrap();
    writeln!(f, "    pub fn min_version(&self) -> i16 {{").unwrap();
    writeln!(f, "        match self {{").unwrap();
    for (_, (name, min_ver, _, _)) in &entries {
        writeln!(f, "            ApiKey::{name} => {min_ver},").unwrap();
    }
    writeln!(f, "            ApiKey::Unknown(_) => 0,").unwrap();
    writeln!(f, "        }}").unwrap();
    writeln!(f, "    }}").unwrap();
    writeln!(f).unwrap();

    // is_flexible_version
    writeln!(f, "    /// Returns true if this API key supports flexible versions and the").unwrap();
    writeln!(f, "    /// given version uses compact string/array encoding.").unwrap();
    writeln!(f, "    pub fn is_flexible_version(&self, version: i16) -> bool {{").unwrap();
    writeln!(f, "        match self {{").unwrap();
    for (_, (name, _, _, flex_min)) in &entries {
        match flex_min {
            Some(min) => writeln!(f, "            ApiKey::{name} => version >= {min},").unwrap(),
            None => writeln!(f, "            ApiKey::{name} => false,").unwrap(),
        }
    }
    writeln!(f, "            ApiKey::Unknown(_) => false,").unwrap();
    writeln!(f, "        }}").unwrap();
    writeln!(f, "    }}").unwrap();
    writeln!(f, "}}").unwrap();
    writeln!(f).unwrap();

    // Display impl
    writeln!(f, "impl fmt::Display for ApiKey {{").unwrap();
    writeln!(f, "    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {{").unwrap();
    writeln!(f, "        write!(f, \"{{}}\", self.name())").unwrap();
    writeln!(f, "    }}").unwrap();
    writeln!(f, "}}").unwrap();

    drop(f);
    println!("cargo:rerun-if-changed={}", dest_path.display());
}
