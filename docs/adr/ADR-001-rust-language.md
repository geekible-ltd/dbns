# ADR-001: Rust as primary implementation language

- Date: 2026-11-26
- Status: accepted

## Context

NDB is a new distributed database requiring a page-oriented storage engine,
custom binary formats, concurrent index structures and consensus-driven
replication (spec §1).

## Problem

The implementation language must provide fine-grained memory control for a
buffer pool and bounded memory budgets, safe concurrency for the B+Tree and
MVCC layers, and reliable cross-platform operation on Ubuntu Linux and
Windows Server (production) and macOS (development platform).

## Alternatives considered

- C++ — maximum control but high cost of memory-safety defects in a
  long-lived storage engine.
- Go — productive but garbage collection complicates a bounded buffer pool
  and predictable P95 latency.
- Java — mature ecosystem but GC pauses and JVM deployment friction conflict
  with the storage engine's memory requirements.

## Decision

Rust is the primary implementation language, targeting stable toolchain with
edition 2024.

## Consequences

- Ownership and the type system eliminate data races in concurrent index
  code and large classes of memory bugs in page handling.
- No GC: memory budgets are enforceable per subsystem (spec §3.1).
- Longer initial development time for low-level structures such as the
  B+Tree, partially offset by stronger correctness guarantees.

## Risks

- Smaller hiring pool for experienced Rust systems engineers.
- Unsafe code will still be required in places; all `unsafe` blocks require
  justification and targeted testing.
