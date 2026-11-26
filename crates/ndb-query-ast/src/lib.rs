//! Query AST (spec §12.1) — MongoDB-inspired, versioned grammar.
//!
//! The AST is deliberately language- and storage-agnostic: the parser
//! produces it, the planner validates and plans against it, the executor
//! evaluates it. `OPERATORS` is the documented operator catalogue (§12.1).

use ndb_document::Value;

/// Grammar version — bump when operators/semantics change (§12.1).
pub const GRAMMAR_VERSION: u32 = 1;

/// The operator catalogue (§12.2–12.7). `vector` operators exist for
/// grammar completeness — evaluation lands in Phase 7.
pub const OPERATORS: &[&str] = &[
    // comparison (§12.2)
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
    // logical (§12.3)
    "$and",
    "$or",
    "$not",
    "$nor",
    // array (§12.4)
    "$all",
    "$elemMatch",
    "$size",
    // existence/type (§12.5)
    "$exists",
    "$type",
    // text (§12.6)
    "$text",
    "$search",
    "$phrase",
    "$fuzzy",
    // vector (§12.7 — Phase 7)
    "$vectorSearch",
    "$vectorFilter",
];

/// Field comparison operators (§12.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Nin,
}

/// Array operators (§12.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrOp {
    All,
    ElemMatch,
    Size,
}

/// A filter expression — a tree of predicates over one document.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `field` (dotted path) compared with `value` (literal or list for
    /// `$in`/`$nin`/`$all`).
    Cmp {
        field: String,
        op: CmpOp,
        value: Value,
    },
    /// `$and`/`$or`/`$nor` over children.
    Logical { op: LogicalOp, children: Vec<Expr> },
    /// `$not` wraps a single predicate.
    Not { inner: Box<Expr> },
    /// Array predicate on `field`.
    Array {
        field: String,
        op: ArrOp,
        value: Value,
    },
    /// `$exists` on `field`.
    Exists { field: String, exists: bool },
    /// `$type` — BSON-ish type name or tag.
    Type { field: String, type_name: String },
    /// Full-text match: `$text`/`$search` on a text index (§12.6).
    Text { index: String, query: String },
    /// Phrase search with optional slop (§12.6).
    Phrase {
        index: String,
        field: String,
        terms: String,
        slop: u32,
    },
    /// Vector k-NN search on an HNSW index (§12.7): drives the access
    /// path when present — `vector` is the raw query embedding.
    VectorSearch {
        index: String,
        field: String,
        vector: Vec<f32>,
        k: usize,
        ef: usize,
    },
    /// Vector similarity predicate on a document field (§12.7) — residual
    /// filter: cosine score >= `min_score`.
    VectorFilter {
        field: String,
        vector: Vec<f32>,
        min_score: f32,
    },
    /// Fuzzy match with edit distance + expansion caps (§12.6).
    Fuzzy {
        index: String,
        field: String,
        term: String,
        max_edits: u32,
        max_expansions: usize,
    },
}

/// Logical operators (§12.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
    Nor,
}

/// Sort direction per field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// One sort key.
#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub field: String,
    pub dir: SortDir,
}

/// Projection spec — include-list or exclude-list, never mixed (except
/// `_id`, matching MongoDB semantics).
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// Only these fields (plus id unless excluded).
    Include(Vec<String>),
    /// Everything except these fields.
    Exclude(Vec<String>),
}

/// A validated find-style query against one collection.
#[derive(Debug, Clone, PartialEq)]
pub struct FindQuery {
    pub collection: String,
    pub database: String,
    pub filter: Option<Expr>,
    pub projection: Option<Projection>,
    pub sort: Vec<SortKey>,
    pub limit: Option<u64>,
    pub skip: u64,
}

/// An aggregation pipeline (§12.9).
#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub collection: String,
    pub database: String,
    pub stages: Vec<Stage>,
}

/// Aggregation pipeline stages (§12.9 initial set).
#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    Match {
        filter: Expr,
    },
    Project {
        spec: Vec<ProjectItem>,
    },
    Group {
        key: AggExpr,
        accumulators: Vec<Accumulator>,
    },
    Sort {
        keys: Vec<SortKey>,
    },
    Limit {
        n: u64,
    },
    Skip {
        n: u64,
    },
    Count {
        out_field: String,
    },
    Unwind {
        field: String,
    },
    AddFields {
        fields: Vec<(String, AggExpr)>,
    },
}

/// A projected field — passthrough or computed.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectItem {
    /// Include `path` under the same name.
    Field(String),
    /// `out_name` = evaluated expression.
    Computed { out: String, expr: AggExpr },
    /// Drop `path` (exclusion projection).
    Exclude(String),
}

/// Expressions usable in $group keys, $project, $addFields (§12.9).
#[derive(Debug, Clone, PartialEq)]
pub enum AggExpr {
    /// Field path reference (`$field.path`).
    FieldPath(String),
    /// Literal value.
    Lit(Value),
    /// `$concat` over sub-expressions.
    Concat(Vec<AggExpr>),
    /// `$toString`.
    ToString(Box<AggExpr>),
    /// `$ifNull` — first non-null of two.
    IfNull {
        expr: Box<AggExpr>,
        fallback: Box<AggExpr>,
    },
}

/// A `$group` accumulator (§12.9 initial expression set).
#[derive(Debug, Clone, PartialEq)]
pub struct Accumulator {
    /// Output field name.
    pub out: String,
    pub op: AccOp,
    /// Argument expression (field path or literal); unused for $count-ish ops.
    pub arg: AggExpr,
}

/// Accumulator operators (§12.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccOp {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
}
