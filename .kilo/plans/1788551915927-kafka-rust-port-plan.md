# Kafka Rust Port — Implementation Plan

## Goal

Port Apache Kafka (Java/Scala, Gradle) to Rust (Cargo workspace) using a
**Ship of Theseus** strategy: compile Rust to JVM bytecode via
`rustc_codegen_jvm`, run it alongside the existing Java/Scala code, replace
modules one-by-one, and eventually compile everything natively (Phase B)
and remove the JVM. We are not deleting existing code at this point.

## Constraints (apply to Rust source code)

| Constraint | Rule | Rationale |
|---|---|---|
| Core deps | `tokio`, `thiserror`, `serde`, `tracing`, plus an **external metrics crate** (e.g. `metrics` or `prometheus`) | Core infrastructure + observability + logging |
| Wrapper deps | Well-established crates allowed as **thin wrappers** for domains that are impractical to re-implement: `uuid` (UUID), `flate2` (gzip), `zstd` (zstd), `lz4`/`lz4_flex` (LZ4), `snap` (snappy) | Avoids reimplementing battle-tested codecs |
| Derive deps | `getset` (getter/setter generation) — all struct fields are private, accessed only via generated accessor methods | Enforces encapsulation parity with Java's private-field + getter/setter pattern |
| Virtual dispatch | **No `dyn Trait` / trait objects** | Avoids runtime vtable overhead; use enums or generics |
| Unsafe | Only when required (byte-level FFI, zero-copy I/O). **Every `unsafe` block must have a `// SAFETY:` comment.** | Rust safety first |
| File formats | Wire protocol + on-disk formats must be **byte-identical** | Interop with existing Java brokers/clients |
| Test coverage | Every Java test class must have a Rust equivalent | Behavioral parity |
| Idiomatic Rust | Mirror Java logic but write idiomatic Rust | Maintainability |
| Traceability | Every Rust function, struct, enum, trait, and module must have a `#[rustc_doc_comment = "MIGRATION_SOURCE: <java_or_scala_file_path>"]` or doc-comment line citing the source file it was ported from. If the element is new (no Java source), cite `MIGRATION_SOURCE: (new)` | Enables Ship of Theseus verification — any Rust element can be traced back to its Java origin |

## Two-Phase Migration

### Phase A — `rustc_codegen_jvm` Bridge (Coexistence)

Rust source compiles to JVM bytecode via `rustc_codegen_jvm` (or fork).
Rust classes live on the same classpath as Java classes. No process
boundary; no serialization overhead across the Rust↔Java boundary.

**Kill criterion (by Month 2):** If `rustc_codegen_jvm` cannot:
(a) compile and run a crate that uses `serde` derives, `thiserror`, `getset`,
and a wrapper crate (e.g. `uuid`) with passing tests, OR
(b) inter-call a real Java class (e.g., `java.util.UUID`) from Rust bytecode
and vice-versa,
then **Phase A is abandoned**. The plan collapses to Phase B-only: native Rust
with FFI bridges (JNI or a separate Rust process) for cross-language testing.
Phase B then uses native `serde`/`thiserror`/`getset` without concern for JVM
bytecode compatibility.

### Phase A no-`dyn-Trait` rationale

The `no dyn Trait` rule applies in Phase A **in source code** even though the
JVM backend virtualizes all calls. Reason: the Rust source is the source of
truth for the eventual Phase B native build where `dyn Trait` would cause real
vtable overhead. Keeping the source free of trait objects means Phase B
requires **zero source changes** for dispatch elimination. The engineering tax
in Phase A is minimal because enums + generics are the natural Rust idioms
anyway.

**Constraints during Phase A** (necessarily relaxed vs. final native):
- `tokio` **cannot** be used — it requires native OS threads. Use JVM
  NIO via a thin Rust↔Java interop shim, or synchronous I/O.
- `serde`, `thiserror`, `getset`, and **all wrapper crates** (uuid,
  compression) **work** normally — they compile to standard Rust IR compatible
  with JVM bytecode.
- "No virtual dispatch" applies to **Rust source** — avoid `dyn Trait`.
  The JVM backend makes all calls virtual regardless; that is acceptable.
- `unsafe` is limited to byte-level operations that the JVM codegen
  supports (array indexing, byte buffer manipulation). Every `unsafe`
  block must include a `// SAFETY:` comment.

**Phase A technical constraints (verified during Week 1-2 spike):**
- **Memory model**: Rust ownership/`Drop`/`Arc` must map to JVM GC. Objects
  with `Drop` impls compile to JVM finalizers (known performance hazard).
  Mitigation: avoid `Drop` in Phase A; use explicit `close()`/`dispose()`
  patterns instead. `Arc` reference counting coexists with JVM GC — the Rust
  side manages its own refcounts, the JVM side manages its own heap.
- **Panics vs exceptions**: Rust panics compile to JVM exceptions. They
  must NOT cross the Rust↔Java boundary as unwinds through JVM frames.
  Mitigation: catch_all at FFI boundary; translate to Java exceptions.
- **Proc-macro compatibility**: `serde`, `thiserror`, `getset` are proc-macro
  crates. `rustc_codegen_jvm` must support proc-macro expansion. If it cannot,
  these crates are temporarily replaced by hand-written (non-derive) impls
  during Phase A. The kill criterion (above) tests this explicitly.
- **Monomorphization**: Rust generics are monomorphized (one impl per type).
  `rustc_codegen_jvm` maps this to per-instantiation JVM methods. If the
  backend falls back to type-erased (Java-style) generics, it would silently
  reintroduce virtual dispatch. **Mitigation**: CI verifies that generic
  monomorphization is preserved by checking that `dyn Trait` is not introduced
  in compiled bytecode (scan for vtable indirection). If type erasure is
  detected, enforce `no_trait_objects` lint at Rust-source level as the
  defense.

**Module order (outside-in, lowest risk first):**

| Step | Module (Java/Scala path) | Rust crate | Why first |
|---|---|---|---|
| 1 | `clients/common/protocol` | `kafka-protocol` | `Readable`/`Writable` traits, `ByteBufferAccessor` — pure byte IO, no threading, no tokio |
| 2 | `clients/common/record` | `kafka-record` | On-disk record batch format (magic 2+) — byte-identical format |
| 3 | `clients/common/Uuid` | `kafka-common` | UUID, utility types |
| 4 | `clients/common/errors` | `kafka-errors` | Exception enum ↔ `thiserror` |
| 5 | `clients/common/compress` | `kafka-compress` | Codec (gzip, zstd, lz4, snappy) |
| 6 | `clients/protocol/types` | `kafka-protocol` | `Schema`, `Type`, `Struct`, `TaggedFields` |
| 7 | `storage/internals/*` | `kafka-storage` | Checkpoint files, log indexes, segment format |
| 8 | `clients/NetworkClient` | `kafka-net` | Network I/O — starts needing threading, but still JVM in Phase A |
| 9 | `metadata/`, `raft/` | `kafka-metadata`, `kafka-raft` | KRaft metadata layer |
| 10 | `server/`, `core/` | `kafka-broker` | Broker state machine |
| 11 | Coordinators | `kafka-coords` | Group, txn, share coordinators |
| 12 | `streams/`, `connect/` | various | Higher-level APIs |

**Acceptance gate per module:** All Rust tests pass when compiled to JVM
bytecode, AND cross-language tests confirm byte-for-byte identical output
with the Java implementation for every code path.

### Phase B — Native Rust (JVM Removal)

Once all modules are ported and verified in Phase A:
1. Enable `tokio` for async networking (`tokio::net::TcpListener`,
   `tokio::io`).
2. Replace JVM interop shims with native Rust equivalents.
3. Compile the entire workspace with `rustc` (native target).
4. Remove `rustc_codegen_jvm` from the build pipeline.
5. The JVM is no longer required at runtime.

## Dependency Strategy

### tokio (native phase only — networking, async runtime)
- Map Java `ExecutorService` → `tokio::task::spawn`
- Map Java `Selector`/NIO → `tokio::net::TcpListener` + `tokio::io::AsyncRead`/`AsyncWrite`
- Map Java `Future` → `tokio::future::Future` (via `.await`)
- During Phase A: provide a compatibility shim that delegates to JVM NIO

### thiserror (errors only)
- Java checked + unchecked exceptions → single `Result<T, Error>` pattern
- Define `Error` enum per crate using `#[derive(Error)]`
- No `String`-based error types; all errors are structured enums
- Example mapping: `UnsupportedVersionException` ↔ `ProtocolError::UnsupportedVersion`

### serde (JSON config and message spec parsing only)
- Use `serde::Deserialize` for: message spec JSON files, `server.properties`,
  `log4j2.yaml`, JAAS config, JSON serde for records
- **Do NOT** use serde for Kafka's binary wire protocol
- Binary protocol uses hand-written `Readable`/`Writable` traits (mirroring
  `clients/.../protocol/Readable.java` and `Writable.java`)

### getset — getter/setter generation (all structs)
- Every struct field must be **private**
- Access via generated methods from the `getset` crate: `#[derive(Getters, Setters)]`
  produces `pub fn field_name(&self) -> &FieldType` and
  `pub fn set_field_name(&mut self, val: FieldType)`
- This mirrors Java's private-field + explicit-getter/setter pattern
  (Kafka's `MessageData` classes all use getters)
- Codegen-generated message structs automatically derive `getset::Getters` +
  `getset::Setters`

### Wrapper crates (compression and UUID)
- **`uuid` crate**: Wrap in `kafka-common` as the Rust `Uuid` type.
  Java's `Uuid` uses base64-encoded `toString()` — the wrapper crate
  must preserve this exact string encoding for interoperation.
  Wire format: 16 raw bytes (two `i64`s), unchanged.
- **`flate2`**: gzip codec — wraps zlib bindings. **Must be configured
  with identical compression level (default 6 in Java) and no extra headers.**
  If byte-exactness cannot be achieved, fallback to calling Java's
  `java.util.zip.GZIPOutputStream` via the JVM bridge in Phase A.
- **`zstd`**: zstd codec. **Must use the same framing (zstd format, not
  raw) and compression level.** Java uses Zstandard library internally.
- **`lz4_flex`** (preferred over `lz4`): faster pure-Rust LZ4. Must match
  Java's `Lz4BlockOutputStream` exactly (compressed block format).
- **`snap`**: snappy codec. Must produce byte-identical output to
  `org.apache.kafka.common.compress.SnappyCompressor`.
- **Byte-exactness gate**: Before a wrapper crate is approved, a test
  must demonstrate that compressing a known payload yields byte-identical
  output to the Java implementation. If any codec fails this gate, that
  codec uses the JVM-bytecode-callable Java implementation as a fallback
  in Phase A, and is replaced with a tuned Rust impl in Phase B.

### Unsafe code policy
```rust
// SAFETY: <explanation of why this is sound>. Must reference the
// invariants that make the unsafe operation safe, and must be updated
// if those invariants change.
unsafe {
    // ... unsafe code ...
}
```
- Prohibited in Phase A (JVM bytecode) unless absolutely necessary
  for zero-copy byte buffer manipulation
- A `cargo` lint denies any `unsafe` without a `// SAFETY:` comment
  on the preceding line

**Unsafe allowlist** (concrete list of approved use cases — anything not
in this list requires a KIP-level design review):
1. `byteorder`-style cast / transmute for byte serialization (e.g.,
   `f64` ↔ `u64` bit-pattern reads for IEEE 754 compatibility)
2. `mmap` or `std::ptr` for zero-copy log segment reads (Phase B only)
3. FFI boundary to JVM (Phase A only) or C libraries (Phase B)
4. SIMD intrinsics for CRC32C computation (if std doesn't provide fast enough)
- Each use case is reviewed by a designated reviewer per PR. The `unsafe`
  lint (`deny(unsafe_code)`) is set to `allow` only for explicitly listed
  modules with `#[allow(unsafe_code)]` + a module-level doc explaining why.

### Metrics (external crate allowed)
- Use the `metrics` crate (or `prometheus` crate) for Rust-native metrics
  during Phase B
- During Phase A: Since Rust IS JVM bytecode, the "bridge" is a direct
  method call — Rust code calls Java's `org.apache.kafka.server.metrics`
  Yammer `Metrics` object via the JVM classpath. A `kafka-metrics` crate
  provides a thin Rust API over the Java Yammer `Metrics` instance.
- Phase B transition: Once native, switch to `metrics::counter!()` /
  `metrics::gauge!()` macros; the `kafka-metrics` crate re-exports
  `metrics` API so call-sites don't change.
- `MIGRATION_SOURCE` on every metric registration points to the
  Java `Metrics` class or `KafkaMetric` that it mirrors

## Source Traceability — `MIGRATION_SOURCE` Convention

Every Rust element that originates from Java/Scala **must** carry a doc
comment citing the source file. This is a hard gate — CI rejects any Rust
source file that contains ported logic without the annotation.

### Format

For items ported from an existing Java/Scala file:

```rust
/// Readable/Writable abstraction that mirrors Java's binary I/O interface.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Readable.java
pub trait Readable {
    // ...
}
```

For items that are new (no Java equivalent):

```rust
/// Tokio-backed async network selector (no direct Java equivalent).
///
/// MIGRATION_SOURCE: (new)
pub struct Selector {
    // ...
}
```

For items ported from a Java *generated* class:

```rust
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/message/ProduceRequestData.java
/// (generated from clients/src/main/resources/common/message/ProduceRequest.json)
#[derive(Getters, Setters, PartialEq, Debug)]
pub struct ProduceRequestData {
    // ...
}
```

### What counts as a migrated element

| Java/Scala element | Rust element | Required? |
|---|---|---|
| `class`, `interface`, `enum`, `annotation` | `struct`, `trait`, `enum`, or macro | Yes |
| `public method` | `pub fn` | Yes |
| `private/protected method` | `fn` | Yes |
| `field` (Java getter/setter pair) | field + `getset` accessor | Cite on the struct; accessors inherit |
| `static constant` | `const` / `static` | Yes |
| `package` | Rust module | Yes |
| **New in Rust** (no Java source) | any | `MIGRATION_SOURCE: (new)` |

### Codegen rule

The `kafka-codegen` crate **must** emit `MIGRATION_SOURCE` doc comments for
every generated struct, enum, method, and constant, pointing to the
corresponding Java generated source file (e.g., `ProduceRequestData.java`)
and the JSON spec that drove it.

### Lint enforcement (per-item, not per-file)

CI runs a custom procedural-macro lint (`kafka-lint`) that walks the Rust
AST (not just `grep`) and verifies:
1. **Every `struct`, `enum`, `trait`, `fn`, `const`** has a `MIGRATION_SOURCE`
   doc comment on the immediately preceding doc-attribute or doc-comment.
2. **Generated files** (`src/generated/`) are committed to a `generated/`
   git-checkpoint branch (not main, to avoid bloating the main repo). CI
   runs `cargo run -p kafka-codegen` to regenerate, then runs `kafka-lint`
   on the fresh output. This catches missing traceability in codegen
   output without polluting the main branch with generated files.
3. Files with zero `MIGRATION_SOURCE` lines fail CI — but this is a
   **coarse** fallback check. The primary gate is the AST-level lint.

## Binary I/O — Hand-Written, Codegen-Driven

The JSON message specs in
`clients/src/main/resources/common/message/*.json` are the source of truth
for Kafka's wire protocol. A Rust codegen tool (separate `codegen` crate)
reads these specs and generates:

1. **Rust structs** for each message type (e.g., `ProduceRequest`,
   `FetchResponse`), with fields matching the JSON spec exactly.
   All fields are private with `getset::Getters`/`getset::Setters` derive.
2. **`read`/`write` methods** implementing the `Readable`/`Writable` traits
   — this mirrors the Java `MessageDataGenerator.java` code
3. **Version-conditional serialization** — fields appear only in versions
   specified by the `versions` / `nullableVersions` / `flexibleVersions`
   attributes, exactly as Java does
4. **Compact vs. non-compact** serialization for flexible versions

The generated Rust code must produce byte-identical output to the Java
generated code. Verification is done via cross-language byte comparison tests.

### Macro-based codegen (preferred approach)

Instead of a standalone codegen tool that emits `.rs` files, prefer Rust
**proc-macros** for code generation. Each message module uses a custom
attribute macro:

```rust
#[kafka_message("ProduceRequest.json")]
pub struct ProduceRequest;
```

The proc-macro reads the JSON spec at compile time and generates the struct,
`getset` accessors, `Readable`/`Writable` impls, version-conditional
serialization, and `MIGRATION_SOURCE` doc comments — all inline.

**Compile-time-only deps** (not in the runtime dependency list, excluded from
the production binary):
- `serde_json` — parse JSON message specs during macro expansion
- `proc-macro2`, `quote` — token-stream generation
- `serde` with `derive` feature — parse JSON spec structs

These crate dependencies exist **only** in `kafka-codegen`'s `Cargo.toml` as
`[dev-dependencies]` or build-time deps; they do not appear in any runtime
crate's dependency tree. The runtime crates (`kafka-protocol`, `kafka-record`,
etc.) depend only on `tokio`, `thiserror`, `serde`, `getset`, and wrapper
crates.

**Fallback**: If `rustc_codegen_jvm` cannot support proc-macros (see kill
criterion), fall back to the standalone `kafka-codegen` tool that emits
`.rs` files at build time via a `build.rs` script. The generated files are
identical either way; only the invocation mechanism changes.

### Readable/Writable trait design (Rust)

```rust
// Mirrors Java Readable/Writable interfaces
pub trait Readable {
    fn read_byte(&mut self) -> io::Result<u8>;
    fn read_short(&mut self) -> io::Result<i16>;
    fn read_int(&mut self) -> io::Result<i32>;
    fn read_long(&mut self) -> io::Result<i64>;
    fn read_double(&mut self) -> io::Result<f64>;
    fn read_array(&mut self, len: usize) -> io::Result<Vec<u8>>;
    fn read_unsigned_varint(&mut self) -> io::Result<u32>;
    fn read_varint(&mut self) -> io::Result<i32>;
    fn read_varlong(&mut self) -> io::Result<i64>;
    fn read_string(&mut self, len: usize) -> io::Result<String>;
    fn read_uuid(&mut self) -> io::Result<Uuid>;
    fn remaining(&self) -> usize;
}

pub trait Writable {
    fn write_byte(&mut self, val: u8);
    fn write_short(&mut self, val: i16);
    fn write_int(&mut self, val: i32);
    fn write_long(&mut self, val: i64);
    fn write_double(&mut self, val: f64);
    fn write_byte_array(&mut self, arr: &[u8]);
    fn write_unsigned_varint(&mut self, val: u32);
    fn write_varint(&mut self, val: i32);
    fn write_varlong(&mut self, val: i64);
    fn write_uuid(&mut self, uuid: &Uuid);
}
```

These traits are generic — no `dyn Readable`/`dyn Writable` trait objects.
Callers use generic type parameters: `fn serialize<W: Writable>(w: &mut W, ...)`.

## Language Mapping Guide

| Java/Scala | Rust | Notes |
|---|---|---|
| `class Foo { private int x; }` | `#[derive(Getters, Setters)] struct Foo { x: i32 }` | Fields private; access via generated `.x()` / `.set_x()` |
| `getter`/`setter` | `getset` generated methods | `foo.x()`, `foo.set_x(val)` — never direct field access |
| `interface Readable` (single impl) | `trait Readable` with generics | No trait objects; pass `impl Readable` |
| `interface Readable` (multi impl: ByteBuffer, DataOutputStream) | `enum ReadableKind` or generic `impl Readable for ByteBufferAccessor` | Enums when variants are fixed; generics when caller can choose |
| `abstract class` | `trait` + struct composition | No inheritance, use composition |
| `extends` (subclass) | Struct embedding | `struct Bar { foo: Foo, extra: i32 }` |
| `ArrayList<T>` | `Vec<T>` | Preserve insertion order |
| `HashMap<K,V>` | `HashMap<K,V>` or `BTreeMap` | Use `BTreeMap` when ordered iteration is needed (mirrors `TreeMap`) |
| `LinkedHashMap<K,V>` | `IndexMap`-equivalent via insertion-ordered `HashMap` + `Vec<(K,V)>` | No external deps; implement minimal ordered map |
| `EnumSet` | `BitFlags` (u64) or enum + match | Use bitset when it maps to Java EnumSet |
| Checked exception | `Result<T, Error>` | All errors are `Result`; no exceptions |
| `RuntimeException` | `Error` enum variant | Use `thiserror` |
| `synchronized` block | `tokio::sync::Mutex` | In native phase; JVM monitor in Phase A |
| `java.util.concurrent.Future` | `tokio::task::JoinHandle` or `oneshot` channel | Native phase |
| `Iterator<T>` | `impl Iterator<Item=T>` or own iterator struct | Rust iterators are preferred |
| `Optional<T>` | `Option<T>` | Direct mapping |
| `null` | `None` | No null pointers |

## No Virtual Dispatch — Replacement Patterns

### Pattern 1: Enum dispatch (for closed sets of types)

Java uses interfaces like `RecordBatch.Reader` implemented by multiple
classes. In Rust, replace with an enum:

```rust
// Instead of dyn RecordBatchReader:
enum RecordBatchKind {
    Default(DefaultRecordBatch),
    Legacy(LegacyRecord),
}
impl RecordBatchTrait for RecordBatchKind { ... }
```

### Pattern 2: Generic dispatch (for open sets)

When the caller doesn't need to know the concrete type, use generics:

```rust
fn serialize_message<R: Readable, W: Writable>(readable: &mut R, writable: &mut W) -> Result<()> { ... }
```

### Pattern 3: Struct composition

Instead of `class A extends B`, use composition:

```rust
struct NetworkClient {
    base: KafkaClientBase,  // "extends"
    selector: Selector,
}
```

## Test Strategy

### Test file mapping

For every Java test class `FooTest.java` at:
```
<module>/src/test/java/org/apache/kafka/.../FooTest.java
```

Create a Rust test module at:
```
crates/<rust-crate>/tests/<foo_test>.rs
```

or inline in the crate:
```rust
#[cfg(test)]
mod tests { ... }
```

### Test framework

- Use Rust's built-in `#[test]` attribute and `assert_eq!` / `assert!` macros
- Use `#[tokio::test]` for async tests (native phase, Phase B)
- During Phase A (JVM bytecode): use JUnit-compatible Rust test harness
  or plain `#[test]` compiled to JVM bytecode
- `#[derive(PartialEq, Debug)]` on all data structs for assertion ergonomics
- Map `@ParameterizedTest` → table-driven `#[test]`
- All test files must also derive `getset` accessors where they construct
  structs with private fields

### Cross-language verification

Every module must pass three test tiers:

1. **Unit tests** (Rust-only): Test each function/struct in isolation
2. **Protocol tests**: Verify byte-for-byte compatibility with Java output.
   Java test vectors are exported as JSON/YAML fixtures; Rust tests
   assert the Rust implementation produces identical bytes.
3. **Integration tests**: Rust module communicates with a running Java Kafka
   broker and verifies round-trip behavior over the real wire protocol

### Coverage gating

- For each Java test method annotated `@Test`, there must be one `#[test]`
  function with the same name in the Rust test module. This is a **structural
  parity** gate, not a numeric gate.
- `@ParameterizedTest` with N cases → one `#[test]` function per case
  (each case is a separate `#[test]`), named `<MethodName>_<CaseIndex>`.
  This ensures every Java test case has a Rust counterpart.
- Java `@Test(expected = SomeException.class)` → Rust `#[test]` with
  `assert!(matches!(result, Err(Error::SomeVariant)))`.
- **CI check**: A script parses both Java test files and Rust test files,
  extracts method/case names, and asserts 1:1 structural parity. No
  test name in Java may lack a Rust twin.
- All test files must also derive `getset` accessors where they construct
  structs with private fields

## File Format Compatibility

### Wire protocol (binary, over TCP)

- Source of truth: `clients/src/main/resources/common/message/*.json` (120+ files)
- Generated from specs by `kafka-codegen` crate
- Must match Java's `MessageDataGenerator` output byte-for-byte
- Verify via cross-language round-trip tests

### On-disk log format

- Record batch format (magic 2+): see `DefaultRecordBatch.java`
- Record format: `DefaultRecord.java`, `MemoryRecords.java`
- Segment format, offset index, time index, transaction index
- Checkpoint files: `OffsetCheckpointFile.java`,
  `LeaderEpochCheckpointFile.java`, `SnapshotFile.java`

### Config formats

- `server.properties` (Java properties format) — parse with serde + custom
  deserializer
- `log4j2.yaml` → Rust `tracing` config (format stays YAML)
- JSON specs → serde Deserialize
- JAAS config → custom parser (small, self-contained)

### Metadata log records

- `core/src/main/resources/metadata/` — JSON specs for KRaft metadata records
- Same codegen approach as wire protocol

## Gradle to Cargo Migration

### Workspace structure

```
kafka/
├── Cargo.toml          (workspace root)
├── crates/
│   ├── kafka-protocol/       (Readable, Writable, ByteBufferAccessor, Errors, Message)
│   ├── kafka-codegen/        (proc-macro: JSON spec → Rust structs)
│   ├── kafka-lint/           (CI: MIGRATION_SOURCE + no-dyn-Trait + unsafe checks)
│   ├── kafka-record/         (record batches, MemoryRecords, DefaultRecord)
│   ├── kafka-common/         (Uuid wrapper, utilities, ordered maps)
│   ├── kafka-compress/       (gzip/zstd/lz4/snappy — wraps flate2, zstd, lz4_flex, snap)
│   ├── kafka-storage/        (log segments, checkpoints, indexes)
│   ├── kafka-net/            (NetworkClient, Selector, I/O — tokio in Phase B)
│   ├── kafka-metadata/       (KRaft metadata log)
│   ├── kafka-raft/           (Raft consensus)
│   ├── kafka-broker/         (broker state machine, log, replication)
│   ├── kafka-coords/         (coordinators: group, txn, share)
│   ├── kafka-metrics/        (metrics facade — yarn bridge Phase A, native Phase B)
│   ├── kafka-streams/        (Kafka Streams DSL)
│   ├── kafka-connect/        (Kafka Connect)
│   └── kafka-cli/            (shell, tools)
```

### Build commands

| Gradle | Cargo |
|---|---|
| `./gradlew clients:jar` | `cargo build -p kafka-protocol` |
| `./gradlew test` | `cargo test` |
| `./gradlew processMessages` | `cargo run -p kafka-codegen` |
| `./gradlew releaseTarGz` | `cargo build --release` |

### Lint configuration (workspace-level `clippy.toml`)

```toml
# Deny trait objects (no virtual dispatch)
clippy::disallowed_types = { "std::sync::Arc<dyn *>" = "use enum or generics instead" }
```

```rust
// In lib.rs of each crate:
#![deny(clippy::rc_buffer)]
#![deny(clippy::arc_with_nomux)]
#![deny(clippy::ptr_arg)]
```

Additionally:
- `RUSTFLAGS="-W clippy::disallowed_types"` in CI denies `dyn Trait`
- A `deny(unsafe_code)` lint pass runs in CI; `unsafe` is only allowed
  in explicitly allowlisted modules with `#[allow(unsafe_code)]` +
  `// SAFETY:` comment per block
- The `kafka-lint` crate (AST-level) enforces `MIGRATION_SOURCE` per-item
  (see Source Traceability section)
- Build-time-only deps (`serde_json`, `quote`, `proc-macro2`) are denied
  in runtime crate `Cargo.toml` via a `deny(dep)` CI check

### Codegen integration

- `kafka-codegen` is a **proc-macro crate** that emits `#[kafka_message("...")]`
  attribute macros for each JSON spec
- OR, as fallback: reads the same JSON specs from `clients/src/main/resources/common/message/`
  and a `build.rs` script writes generated `.rs` files to `crates/kafka-protocol/src/generated/`
- `build.rs` in each crate triggers codegen (macro or fallback) when specs change
- `kafka-codegen` is a normal Cargo crate; its proc-macro deps (`serde_json`,
  `quote`, `proc-macro2`) are **build-time only** and do not leak into runtime
  dependency trees

## Module Porting Checklist (per crate)

For each module:

1. **Create crate skeleton** — `Cargo.toml` (core deps: tokio, thiserror, serde; wrapper deps as needed; `getset` for derives)
2. **Port `Readable`/`Writable` layer** — byte IO primitives
3. **Run codegen** — generate message structs from JSON specs (with `getset` derives + `MIGRATION_SOURCE` comments)
4. **Implement domain structs** — record format, log segments, etc. (all fields private, `getset` accessors, `MIGRATION_SOURCE` on every type/fn/method/const)
5. **Translate error types** — Java exceptions → `thiserror` enum
6. **Translate utility classes** — `ByteUtils`, `Utils`, etc.
7. **Traceability gate**: every Rust element cites `MIGRATION_SOURCE:` (or `(new)`)
8. **Lint gate**: `cargo clippy` passes, no `dyn Trait`, all `unsafe` has `// SAFETY:` comments
9. **Write unit tests** — one Rust test per Java test method
10. **Write cross-language tests** — verify byte-identical output
11. **CI gate** — all tests pass, coverage parity verified
12. **Deprecate Java source** — mark Java classes as `@Deprecated`

## Rollout / Migration Path

1. **Week 1-2**: Create `Cargo.toml` workspace, `kafka-protocol` crate skeleton,
   `kafka-codegen` crate that parses JSON specs. Establish `getset` +
   lint rules.
2. **Week 3-4**: Generate `Readable`/`Writable` + first 5 message types
   (Produce, Fetch, ApiVersions). Verify byte-for-byte with Java.
   Establish cross-language test harness.
3. **Month 1**: Complete protocol layer, start record format. rustc_codegen_jvm
   setup for JVM integration testing.
4. **Month 2-3**: Storage layer (checkpoint files, log indexes). Cross-language
   tests with real Kafka data directories.
5. **Month 4+**: Network layer (Phase B: tokio async I/O), broker internals, Raft, coordinators.
6. **Year 1+**: Streams, Connect, tools. Phase B native compilation and JVM removal.

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| `rustc_codegen_jvm` is experimental/unmaintained | Identify a fork or plan to maintain it; keep Phase A modules compilable natively as fallback |
| tokio incompatible with JVM bytecode | Phase A: use JVM NIO shim; Phase B: enable tokio |
| Java reflection used in config/codegen | Rust: codegen-time reflection; runtime: explicit field access |
| Scala code in `core/` (broker) | Scala interop is two-way: Rust bytecode must be callable FROM Scala (to replace Scala modules). Validate this in the Month 1 spike. If it fails, Scala modules stay Java-side longer |
| Operational tooling compatibility | Existing `kafka-topics.sh`, Zookeeper, JMX consumers, Cruise Control, etc. must work with a hybrid Java/Rust broker. Ensure Rust modules expose the same JMX beans via the Yammer bridge in Phase A. Phase B: native HTTP metrics endpoint |
| Large number of modules (~40) | Ship of Theseus — replace one at a time; cluster stays running |
| Test framework differences (JUnit vs Rust test) | Map `@Test` → `#[test]`, `@ParameterizedTest` → table-driven `#[test]` |
| Mock testing (Mockito → Rust) | Replace Mockito with hand-written stub impls behind traits; no external mock deps |
| Ordered collections (`LinkedHashMap`) | Implement minimal `OrderedHashMap` in `kafka-common` (Vec + HashMap). Do NOT use `indexmap` crate — keep dependencies minimal per the wrapper-crate policy |
| GC → manual memory | Rust ownership model replaces GC; use `Arc` for shared ownership, `Rc` for single-threaded |
| Java threads → async/await | tokio tasks + channels; JVM thread pool in Phase A |
| Wrapper crate version drift | Pin exact versions in `Cargo.toml`; re-export only Kafka-native types |
| getset API mismatch vs Java | Generated getters return `&T` (references); Java returns boxed types. Verify null-equivalent (`Option`) handling per field |
| Unsafe audit burden | `// SAFETY:` comment required on every `unsafe` block; CI gate denies unannotated unsafe |
| Yammer metrics interoperation | Add a Yammer metrics bridge crate for Phase A (Rust reports metrics via Java Yammer); Phase B uses native `metrics` or `prometheus` crate |

## Open Questions (resolve during implementation)

1. **rustc_codegen_jvm status**: Is there an actively maintained fork? If not,
   we may need to maintain our own or use a different JVM bridge (FFI/JNI).
2. **Scala→Rust mapping**: How to handle Scala-specific features (pattern matching,
   implicits, case classes) in the `core/` module?
3. **Config hot-reload**: Java uses `DynamicConfigChanged` events; Rust needs
   an equivalent `tokio::sync::watch` channel pattern.
4. **Metrics**: Java uses Yammer metrics; Phase A: yammer-metrics-bridge
   forwards to Java. Phase B: use `metrics` or `prometheus` crate (resolved:
   external metrics crate permitted).
5. **JMX**: Java exposes metrics via JMX; Rust will use a native protocol
   (likely HTTP or Prometheus text format). Phase A: yammer-metrics-bridge
   forwards Rust metrics to Java Yammer for existing dashboards.
6. **Compression codec byte-exactness**: `flate2`/`zstd`/`lz4_flex`/`snap`
   must produce byte-identical output to Kafka's Java compression. Some
   codecs may require wrapper tuning (e.g., compression level, header format).
7. **getset return types**: Java getters that return `null` (e.g., nullable
   fields) → Rust `Option<&T>`. Need a codegen rule: nullable fields generate
   `Option` in Rust.

### Resolved decisions
- **Metrics crate**: An external metrics crate (`metrics` or `prometheus`)
  is permitted beyond the three core deps. Phase A uses a Yammer bridge.
- **No virtual dispatch**: `no_trait_objects` lint denies `dyn Trait`
  in Rust source. JVM backend virtual dispatch is acceptable.
- **Logging**: `tracing` crate is permitted (replaces log4j2). Phase A
  delegates to Java's log4j2 via JVM bridge; Phase B uses native `tracing`.
- **Codegen macros**: Proc-macro approach is preferred (compile-time-only
  deps: `serde_json`, `quote`, `proc-macro2`). Fallback: standalone codegen
  tool with `build.rs`.
