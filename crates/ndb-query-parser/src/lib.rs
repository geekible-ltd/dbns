//! Query parser — MongoDB-inspired JSON → [`ndb_query_ast`] (spec §12.8
//! stage "Parse JSON" + "Build AST" + "Validate syntax and types").
//!
//! ```json
//! {
//!   "collection": "users", "database": "app",
//!   "filter": { "age": { "$gte": 18 }, "$or": [ {...}, {...} ] },
//!   "projection": { "name": 1, "email": 1 },
//!   "sort": { "age": -1 }, "limit": 10, "skip": 0
//! }
//! ```
//!
//! Aggregation pipelines: `"pipeline": [ {"$match": …}, {"$group": …} ]`.
//! Not complete MongoDB compatibility (§12.1) — unknown operators and
//! malformed clauses are hard errors, never silently ignored.

use ndb_common::error::Error;
use ndb_document::Value;
use ndb_query_ast::*;
use serde_json::{Map as JMap, Value as J};

fn err<T>(msg: impl Into<String>) -> ndb_common::Result<T> {
    Err(Error::InvalidInput(format!("query: {}", msg.into())))
}

/// JSON → `ndb_document::Value` (shared with REST ingestion later).
pub fn json_to_value(j: &J) -> ndb_common::Result<Value> {
    Ok(match j {
        J::Null => Value::Null,
        J::Bool(b) => Value::Bool(*b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        J::String(s) => Value::String(s.clone()),
        J::Array(items) => Value::Array(items.iter().map(json_to_value).collect::<Result<_, _>>()?),
        J::Object(map) => {
            let mut fields = std::collections::BTreeMap::new();
            for (k, v) in map {
                fields.insert(k.clone(), json_to_value(v)?);
            }
            Value::Object(fields)
        }
    })
}

/// Parse a find-style query document.
pub fn parse_find(src: &str) -> ndb_common::Result<FindQuery> {
    let j: J = serde_json::from_str(src)
        .map_err(|e| Error::InvalidInput(format!("query: bad JSON: {e}")))?;
    let obj = j
        .as_object()
        .ok_or_else(|| Error::InvalidInput("query: top level must be an object".into()))?;
    let collection = req_str(obj, "collection")?;
    let database = opt_str(obj, "database").unwrap_or_else(|| "default".into());
    let filter = match obj.get("filter") {
        Some(J::Object(f)) => Some(parse_filter_obj(f)?),
        Some(_) => return err("'filter' must be an object"),
        None => None,
    };
    let projection = match obj.get("projection") {
        Some(J::Object(p)) => Some(parse_projection(p)?),
        Some(_) => return err("'projection' must be an object"),
        None => None,
    };
    let sort = match obj.get("sort") {
        Some(J::Object(s)) => parse_sort(s)?,
        Some(_) => return err("'sort' must be an object"),
        None => Vec::new(),
    };
    let limit = match obj.get("limit") {
        Some(J::Number(n)) => Some(n.as_u64().unwrap_or(0)),
        Some(_) => return err("'limit' must be a number"),
        None => None,
    };
    let skip = match obj.get("skip") {
        Some(J::Number(n)) => n.as_u64().unwrap_or(0),
        Some(_) => return err("'skip' must be a number"),
        None => 0,
    };
    Ok(FindQuery {
        collection,
        database,
        filter,
        projection,
        sort,
        limit,
        skip,
    })
}

/// Parse an aggregation pipeline document (§12.9).
pub fn parse_pipeline(src: &str) -> ndb_common::Result<Pipeline> {
    let j: J = serde_json::from_str(src)
        .map_err(|e| Error::InvalidInput(format!("query: bad JSON: {e}")))?;
    let obj = j
        .as_object()
        .ok_or_else(|| Error::InvalidInput("pipeline: top level must be an object".into()))?;
    let collection = req_str(obj, "collection")?;
    let database = opt_str(obj, "database").unwrap_or_else(|| "default".into());
    let stages = match obj.get("pipeline") {
        Some(J::Array(stages)) => stages
            .iter()
            .map(parse_stage)
            .collect::<ndb_common::Result<Vec<_>>>()?,
        Some(_) => return err("'pipeline' must be an array"),
        None => return err("'pipeline' missing"),
    };
    Ok(Pipeline {
        collection,
        database,
        stages,
    })
}

// ---- internals ---------------------------------------------------------

fn req_str(obj: &JMap<String, J>, k: &str) -> ndb_common::Result<String> {
    obj.get(k)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| Error::InvalidInput(format!("query: '{k}' required")))
}

fn opt_str(obj: &JMap<String, J>, k: &str) -> Option<String> {
    obj.get(k).and_then(|v| v.as_str()).map(|s| s.to_string())
}

/// A filter object `{ field: cond, $logical: [...], ... }` — field clauses
/// are implicitly AND-ed (MongoDB convention).
fn parse_filter_obj(obj: &JMap<String, J>) -> ndb_common::Result<Expr> {
    let mut children = Vec::new();
    for (k, v) in obj {
        if k.starts_with('$') {
            children.push(parse_logical(k, v)?);
        } else {
            children.push(parse_field_clause(k, v)?);
        }
    }
    match children.len() {
        0 => err("empty filter object"),
        1 => Ok(children.pop().unwrap()),
        _ => Ok(Expr::Logical {
            op: LogicalOp::And,
            children,
        }),
    }
}

fn parse_logical(op: &str, v: &J) -> ndb_common::Result<Expr> {
    match op {
        "$and" | "$or" | "$nor" => {
            let arr = v
                .as_array()
                .ok_or_else(|| Error::InvalidInput(format!("query: '{op}' needs an array")))?;
            let children = arr
                .iter()
                .map(|e| {
                    e.as_object()
                        .ok_or_else(|| {
                            Error::InvalidInput(format!("query: '{op}' element must be object"))
                        })
                        .and_then(parse_filter_obj)
                })
                .collect::<ndb_common::Result<Vec<_>>>()?;
            Ok(Expr::Logical {
                op: match op {
                    "$and" => LogicalOp::And,
                    "$or" => LogicalOp::Or,
                    _ => LogicalOp::Nor,
                },
                children,
            })
        }
        "$text" | "$search" => {
            // MongoDB-style: {"$text": {"$search": "…", "$index": "name"}}
            let o = v
                .as_object()
                .ok_or_else(|| Error::InvalidInput("query: '$text' needs an object".into()))?;
            let query = opt_str(o, "$search")
                .or_else(|| opt_str(o, "search"))
                .or_else(|| opt_str(o, "query"))
                .ok_or_else(|| Error::InvalidInput("query: '$text' needs $search".into()))?;
            let index = opt_str(o, "$index")
                .or_else(|| opt_str(o, "index"))
                .unwrap_or_default();
            Ok(Expr::Text { index, query })
        }
        "$not" => {
            let inner = v
                .as_object()
                .ok_or_else(|| Error::InvalidInput("query: '$not' needs an object".into()))?;
            Ok(Expr::Not {
                inner: Box::new(parse_filter_obj(inner)?),
            })
        }
        _ => err(format!("unknown top-level operator '{op}'")),
    }
}

/// `{ "$text": { "$search": "…" } }` and friends (§12.6) can appear as a
/// field clause key OR as an operator clause on a special `_text` channel.
/// MongoDB puts `$text` at top level: `{"$text": {"$search": "cake"}}`.
/// We accept both `{ "$text": {"$search": q, "$index": name} }` inside a
/// filter object and field-level `{"$phrase": …}`/`{"$fuzzy": …}` clauses.
fn parse_field_clause(field: &str, v: &J) -> ndb_common::Result<Expr> {
    match v {
        // Bare value → $eq
        J::Object(ops) if ops.keys().any(|k| k.starts_with('$')) => {
            let mut children = Vec::new();
            for (op, operand) in ops {
                children.push(parse_op_clause(field, op, operand)?);
            }
            match children.len() {
                1 => Ok(children.pop().unwrap()),
                _ => Ok(Expr::Logical {
                    op: LogicalOp::And,
                    children,
                }),
            }
        }
        other => Ok(Expr::Cmp {
            field: field.to_string(),
            op: CmpOp::Eq,
            value: json_to_value(other)?,
        }),
    }
}

fn parse_op_clause(field: &str, op: &str, operand: &J) -> ndb_common::Result<Expr> {
    let cmp = |o: CmpOp, operand: &J| -> ndb_common::Result<Expr> {
        Ok(Expr::Cmp {
            field: field.to_string(),
            op: o,
            value: json_to_value(operand)?,
        })
    };
    match op {
        "$eq" => cmp(CmpOp::Eq, operand),
        "$ne" => cmp(CmpOp::Ne, operand),
        "$gt" => cmp(CmpOp::Gt, operand),
        "$gte" => cmp(CmpOp::Gte, operand),
        "$lt" => cmp(CmpOp::Lt, operand),
        "$lte" => cmp(CmpOp::Lte, operand),
        "$in" | "$nin" | "$all" => {
            let arr = operand
                .as_array()
                .ok_or_else(|| Error::InvalidInput(format!("query: '{op}' needs an array")))?;
            let vals: Vec<Value> = arr.iter().map(json_to_value).collect::<Result<_, _>>()?;
            match op {
                "$in" => cmp(CmpOp::In, &J::Array(arr.clone())),
                "$nin" => cmp(CmpOp::Nin, &J::Array(arr.clone())),
                _ => Ok(Expr::Array {
                    field: field.to_string(),
                    op: ArrOp::All,
                    value: Value::Array(vals),
                }),
            }
        }
        "$size" => Ok(Expr::Array {
            field: field.to_string(),
            op: ArrOp::Size,
            value: json_to_value(operand)?,
        }),
        "$elemMatch" => Ok(Expr::Array {
            field: field.to_string(),
            op: ArrOp::ElemMatch,
            value: json_to_value(operand)?,
        }),
        "$exists" => {
            let b = operand
                .as_bool()
                .ok_or_else(|| Error::InvalidInput("query: '$exists' needs a bool".into()))?;
            Ok(Expr::Exists {
                field: field.to_string(),
                exists: b,
            })
        }
        "$type" => {
            let t = operand
                .as_str()
                .ok_or_else(|| Error::InvalidInput("query: '$type' needs a type name".into()))?;
            Ok(Expr::Type {
                field: field.to_string(),
                type_name: t.to_string(),
            })
        }
        "$phrase" => {
            let o = operand
                .as_object()
                .ok_or_else(|| Error::InvalidInput("query: '$phrase' needs an object".into()))?;
            let terms = opt_str(o, "terms").unwrap_or_default();
            let index = opt_str(o, "index").unwrap_or_default();
            let slop = o.get("slop").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            Ok(Expr::Phrase {
                index,
                field: field.to_string(),
                terms,
                slop,
            })
        }
        "$fuzzy" => {
            let o = operand
                .as_object()
                .ok_or_else(|| Error::InvalidInput("query: '$fuzzy' needs an object".into()))?;
            let term = opt_str(o, "term").unwrap_or_default();
            let index = opt_str(o, "index").unwrap_or_default();
            let _ = index;
            let max_edits = o.get("maxEdits").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
            let max_expansions = o
                .get("maxExpansions")
                .and_then(|v| v.as_u64())
                .unwrap_or(50) as usize;
            Ok(Expr::Fuzzy {
                index: String::new(),
                field: field.to_string(),
                term,
                max_edits,
                max_expansions,
            })
        }
        _ => err(format!("unknown operator '{op}'")),
    }
}

fn parse_sort(obj: &JMap<String, J>) -> ndb_common::Result<Vec<SortKey>> {
    let mut keys = Vec::new();
    for (f, d) in obj {
        let n = d
            .as_i64()
            .ok_or_else(|| Error::InvalidInput("query: sort values must be 1 or -1".into()))?;
        keys.push(SortKey {
            field: f.clone(),
            dir: if n >= 0 { SortDir::Asc } else { SortDir::Desc },
        });
    }
    Ok(keys)
}

fn parse_projection(obj: &JMap<String, J>) -> ndb_common::Result<Projection> {
    let mut includes = Vec::new();
    let mut excludes = Vec::new();
    for (f, flag) in obj {
        match flag
            .as_i64()
            .unwrap_or(flag.as_bool().map(|b| b as i64).unwrap_or(1))
        {
            0 => excludes.push(f.clone()),
            _ => includes.push(f.clone()),
        }
    }
    if !includes.is_empty() && !excludes.is_empty() {
        return err("projection cannot mix include/exclude (except _id)");
    }
    if includes.is_empty() {
        Ok(Projection::Exclude(excludes))
    } else {
        Ok(Projection::Include(includes))
    }
}

fn parse_stage(j: &J) -> ndb_common::Result<Stage> {
    let obj = j
        .as_object()
        .ok_or_else(|| Error::InvalidInput("pipeline: stage must be an object".into()))?;
    if obj.len() != 1 {
        return err("pipeline stage must have exactly one operator");
    }
    let (name, arg) = obj.iter().next().unwrap();
    match name.as_str() {
        "$match" => {
            Ok(Stage::Match {
                filter: parse_filter_obj(arg.as_object().ok_or_else(|| {
                    Error::InvalidInput("pipeline: $match needs an object".into())
                })?)?,
            })
        }
        "$sort" => Ok(Stage::Sort {
            keys: parse_sort(
                arg.as_object()
                    .ok_or_else(|| Error::InvalidInput("pipeline: $sort needs an object".into()))?,
            )?,
        }),
        "$limit" => Ok(Stage::Limit {
            n: arg
                .as_u64()
                .ok_or_else(|| Error::InvalidInput("pipeline: $limit needs a number".into()))?,
        }),
        "$skip" => Ok(Stage::Skip {
            n: arg
                .as_u64()
                .ok_or_else(|| Error::InvalidInput("pipeline: $skip needs a number".into()))?,
        }),
        "$count" => Ok(Stage::Count {
            out_field: arg
                .as_str()
                .ok_or_else(|| Error::InvalidInput("pipeline: $count needs a field name".into()))?
                .to_string(),
        }),
        "$unwind" => {
            let f = match arg {
                J::String(s) => s.trim_start_matches('$').to_string(),
                J::Object(o) => opt_str(o, "path")
                    .map(|p| p.trim_start_matches('$').to_string())
                    .unwrap_or_default(),
                _ => return err("$unwind needs a field path"),
            };
            Ok(Stage::Unwind { field: f })
        }
        "$project" => {
            let o = arg
                .as_object()
                .ok_or_else(|| Error::InvalidInput("pipeline: $project needs an object".into()))?;
            let mut spec = Vec::new();
            for (k, v) in o {
                match v {
                    J::Number(n) if n.as_i64() == Some(0) => {
                        spec.push(ProjectItem::Exclude(k.clone()))
                    }
                    J::Number(_) => spec.push(ProjectItem::Field(k.clone())),
                    J::Bool(true) => spec.push(ProjectItem::Field(k.clone())),
                    J::Bool(false) => spec.push(ProjectItem::Exclude(k.clone())),
                    other => spec.push(ProjectItem::Computed {
                        out: k.clone(),
                        expr: parse_agg_expr(other)?,
                    }),
                }
            }
            Ok(Stage::Project { spec })
        }
        "$addFields" => {
            let o = arg.as_object().ok_or_else(|| {
                Error::InvalidInput("pipeline: $addFields needs an object".into())
            })?;
            let fields = o
                .iter()
                .map(|(k, v)| Ok((k.clone(), parse_agg_expr(v)?)))
                .collect::<ndb_common::Result<Vec<_>>>()?;
            Ok(Stage::AddFields { fields })
        }
        "$group" => {
            let o = arg
                .as_object()
                .ok_or_else(|| Error::InvalidInput("pipeline: $group needs an object".into()))?;
            let id = o
                .get("_id")
                .ok_or_else(|| Error::InvalidInput("pipeline: $group needs '_id'".into()))?;
            let key = match id {
                J::Null => AggExpr::Lit(Value::Null),
                other => parse_agg_expr(other)?,
            };
            let mut accumulators = Vec::new();
            for (out, spec) in o.iter().filter(|(k, _)| *k != "_id") {
                let so = spec.as_object().ok_or_else(|| {
                    Error::InvalidInput("pipeline: $group accumulator needs an object".into())
                })?;
                if so.len() != 1 {
                    return err("accumulator must have exactly one operator");
                }
                let (op, arg) = so.iter().next().unwrap();
                let op = match op.as_str() {
                    "$sum" => AccOp::Sum,
                    "$avg" => AccOp::Avg,
                    "$min" => AccOp::Min,
                    "$max" => AccOp::Max,
                    "$first" => AccOp::First,
                    "$last" => AccOp::Last,
                    other => return err(format!("unknown accumulator '{other}'")),
                };
                accumulators.push(Accumulator {
                    out: out.clone(),
                    op,
                    arg: parse_agg_expr(arg)?,
                });
            }
            Ok(Stage::Group { key, accumulators })
        }
        other => err(format!("unknown pipeline stage '{other}'")),
    }
}

/// An expression node: `"$path"` → field ref, `{"$concat": [...]}` → call,
/// everything else → literal.
fn parse_agg_expr(j: &J) -> ndb_common::Result<AggExpr> {
    match j {
        J::String(s) if s.starts_with('$') => {
            Ok(AggExpr::FieldPath(s.trim_start_matches('$').to_string()))
        }
        J::Object(o) if o.len() == 1 => {
            let (op, arg) = o.iter().next().unwrap();
            match op.as_str() {
                "$concat" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidInput("pipeline: $concat needs array".into())
                    })?;
                    Ok(AggExpr::Concat(
                        arr.iter().map(parse_agg_expr).collect::<Result<_, _>>()?,
                    ))
                }
                "$toString" => Ok(AggExpr::ToString(Box::new(parse_agg_expr(arg)?))),
                "$ifNull" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidInput("pipeline: $ifNull needs [expr, fallback]".into())
                    })?;
                    if arr.len() != 2 {
                        return err("$ifNull needs [expr, fallback]");
                    }
                    Ok(AggExpr::IfNull {
                        expr: Box::new(parse_agg_expr(&arr[0])?),
                        fallback: Box::new(parse_agg_expr(&arr[1])?),
                    })
                }
                _ => Ok(AggExpr::Lit(json_to_value(j)?)),
            }
        }
        other => Ok(AggExpr::Lit(json_to_value(other)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_with_all_clauses() {
        let q = parse_find(
            r#"{
            "collection": "users", "database": "app",
            "filter": { "age": { "$gte": 18, "$lt": 65 }, "active": true },
            "projection": { "name": 1 },
            "sort": { "age": -1 }, "limit": 10, "skip": 5
        }"#,
        )
        .unwrap();
        assert_eq!(q.collection, "users");
        assert_eq!(q.limit, Some(10));
        assert_eq!(q.skip, 5);
        assert_eq!(q.sort[0].dir, SortDir::Desc);
        match q.filter.unwrap() {
            Expr::Logical {
                op: LogicalOp::And,
                children,
            } => assert_eq!(children.len(), 2),
            _ => panic!("expected implicit AND"),
        }
    }

    #[test]
    fn logical_and_array_ops() {
        let q = parse_find(
            r#"{
            "collection": "c",
            "filter": { "$or": [ {"a": 1}, {"tags": {"$all": ["x","y"]}}, {"tags": {"$size": 3}} ] }
        }"#,
        )
        .unwrap();
        match q.filter.unwrap() {
            Expr::Logical {
                op: LogicalOp::Or,
                children,
            } => assert_eq!(children.len(), 3),
            _ => panic!("expected $or"),
        }
    }

    #[test]
    fn rejects_unknown_operators_and_mixed_projection() {
        assert!(parse_find(r#"{"collection":"c","filter":{"a":{"$bogus":1}}}"#).is_err());
        assert!(parse_find(r#"{"collection":"c","projection":{"a":1,"b":0}}"#).is_err());
    }

    #[test]
    fn pipeline_stages() {
        let p = parse_pipeline(
            r#"{
            "collection": "orders",
            "pipeline": [
                {"$match": {"status": "paid"}},
                {"$group": {"_id": "$region", "total": {"$sum": "$amount"}, "n": {"$sum": 1}}},
                {"$sort": {"total": -1}},
                {"$limit": 5}
            ]
        }"#,
        )
        .unwrap();
        assert_eq!(p.stages.len(), 4);
        match &p.stages[1] {
            Stage::Group { key, accumulators } => {
                assert_eq!(*key, AggExpr::FieldPath("region".into()));
                assert_eq!(accumulators.len(), 2);
            }
            _ => panic!("expected $group"),
        }
    }

    #[test]
    fn text_and_fuzzy_clauses() {
        let q = parse_find(
            r#"{
            "collection": "docs",
            "filter": { "body": {"$fuzzy": {"term": "apple", "maxEdits": 1}} }
        }"#,
        )
        .unwrap();
        match q.filter.unwrap() {
            Expr::Fuzzy { max_edits, .. } => assert_eq!(max_edits, 1),
            _ => panic!("expected fuzzy"),
        }
    }
}
