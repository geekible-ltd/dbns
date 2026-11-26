//! Collection metadata persisted in the catalog tree (spec §8.1 — index
//! definitions live with collection metadata).
//!
//! ```text
//! u16 meta_version (1)
//! u64 collection_id
//! u64 pk_root
//! u64 next_doc_id        — monotonic primary-id counter (spec §8.2)
//! u32 field_dict_len | field_dict bytes
//! schema: u32 version | u8 strict | u16 field_count |
//!         ×{ u16 name_len | name | u8 has_type | u8 type_tag | u8 required }
//! u16 index_count | ×IndexDef
//! u8 has_database | u16 db_name_len | db_name
//! u16 coll_name_len | coll_name
//! ```

use ndb_analyzer::Analyzer;
use ndb_common::error::Error;
use ndb_common::ids::{CollectionId, PageId};
use ndb_document::{DataType, FieldDictionary, Schema};

/// A secondary index definition (spec §8.3). `unique` rejects duplicate key
/// tuples; `sparse` skips documents missing an indexed field. Ordered and
/// range access come free from the B+Tree's key order; partial (predicate)
/// indexes wait for the query engine's expression evaluator.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
    pub sparse: bool,
    pub root: PageId,
}

/// One analysed field inside a text index — per-field weight (spec §9.3)
/// plus the versioned analyser config (§9.2: changing it forces a rebuild,
/// never a silent mix of tokenisation rules).
#[derive(Debug, Clone, PartialEq)]
pub struct TextFieldDef {
    pub field: String,
    pub weight: f64,
    /// Encoded `AnalyzerConfig` (versioned).
    pub analyzer_cfg: Vec<u8>,
    /// Analyzer config version pinned at index-create time.
    pub analyzer_version: u32,
}

/// A full-text index definition (spec §9.1): a dedicated tree of term
/// postings over one or more weighted text fields.
#[derive(Debug, Clone, PartialEq)]
pub struct TextIndexDef {
    pub name: String,
    pub fields: Vec<TextFieldDef>,
    pub root: PageId,
}

/// Everything needed to open a collection — persisted as one catalog value.
#[derive(Debug, Clone)]
pub struct CollectionMeta {
    pub id: CollectionId,
    pub database: String,
    pub name: String,
    pub pk_root: PageId,
    /// Next auto-assigned document id (monotonic — B+Tree-friendly).
    pub next_doc_id: u64,
    pub dict: FieldDictionary,
    pub schema: Schema,
    pub indexes: Vec<IndexDef>,
    /// Full-text indexes (spec §9.1).
    pub text_indexes: Vec<TextIndexDef>,
}

impl CollectionMeta {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u16(&mut out, 1); // meta version
        put_u64(&mut out, self.id.get());
        put_u64(&mut out, self.pk_root.get());
        put_u64(&mut out, self.next_doc_id);
        let dict = self.dict.encode();
        put_u32(&mut out, dict.len() as u32);
        out.extend_from_slice(&dict);
        // schema
        put_u32(&mut out, self.schema.version);
        out.push(self.schema.strict as u8);
        let names: Vec<&String> = self.schema.fields().collect();
        put_u16(&mut out, names.len() as u16);
        for name in names {
            let c = self.schema.constraint(name).unwrap();
            put_u16(&mut out, name.len() as u16);
            out.extend_from_slice(name.as_bytes());
            match c.data_type {
                Some(t) => {
                    out.push(1);
                    out.push(t as u8);
                }
                None => {
                    out.push(0);
                    out.push(0);
                }
            }
            out.push(c.required as u8);
        }
        // indexes
        put_u16(&mut out, self.indexes.len() as u16);
        for idx in &self.indexes {
            put_u16(&mut out, idx.name.len() as u16);
            out.extend_from_slice(idx.name.as_bytes());
            put_u16(&mut out, idx.fields.len() as u16);
            for f in &idx.fields {
                put_u16(&mut out, f.len() as u16);
                out.extend_from_slice(f.as_bytes());
            }
            out.push(idx.unique as u8);
            out.push(idx.sparse as u8);
            put_u64(&mut out, idx.root.get());
        }
        put_u16(&mut out, self.database.len() as u16);
        out.extend_from_slice(self.database.as_bytes());
        put_u16(&mut out, self.name.len() as u16);
        out.extend_from_slice(self.name.as_bytes());
        // text indexes (appended — older decoders stop at `name`)
        put_u16(&mut out, self.text_indexes.len() as u16);
        for t in &self.text_indexes {
            put_u16(&mut out, t.name.len() as u16);
            out.extend_from_slice(t.name.as_bytes());
            put_u64(&mut out, t.root.get());
            put_u16(&mut out, t.fields.len() as u16);
            for f in &t.fields {
                put_u16(&mut out, f.field.len() as u16);
                out.extend_from_slice(f.field.as_bytes());
                put_u64(&mut out, f.weight.to_bits());
                put_u32(&mut out, f.analyzer_version);
                put_u32(&mut out, f.analyzer_cfg.len() as u32);
                out.extend_from_slice(&f.analyzer_cfg);
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> ndb_common::Result<Self> {
        let mut r = Reader { buf, pos: 0 };
        let _version = r.u16()?;
        let id = CollectionId::new(r.u64()?);
        let pk_root = PageId::new(r.u64()?);
        let next_doc_id = r.u64()?;
        let dict_len = r.u32()? as usize;
        let dict = FieldDictionary::decode(r.bytes(dict_len)?)?;
        // schema
        let sver = r.u32()?;
        let strict = r.u8()? != 0;
        let nfields = r.u16()? as usize;
        let mut schema = Schema::new();
        schema.version = sver;
        schema.strict = strict;
        for _ in 0..nfields {
            let nlen = r.u16()? as usize;
            let name = String::from_utf8(r.bytes(nlen)?.to_vec())
                .map_err(|_| Error::Corruption("meta: bad utf8 field name".into()))?;
            let has_type = r.u8()? != 0;
            let tag = r.u8()?;
            let required = r.u8()? != 0;
            schema = match (has_type, required) {
                (true, req) => schema.field(
                    &name,
                    DataType::from_tag(tag)
                        .ok_or_else(|| Error::Corruption("meta: bad type tag".into()))?,
                    req,
                ),
                (false, req) => schema.field_any_type(&name, req),
            };
        }
        // indexes
        let nidx = r.u16()? as usize;
        let mut indexes = Vec::with_capacity(nidx);
        for _ in 0..nidx {
            let nlen = r.u16()? as usize;
            let name = String::from_utf8(r.bytes(nlen)?.to_vec())
                .map_err(|_| Error::Corruption("meta: bad utf8 index name".into()))?;
            let nf = r.u16()? as usize;
            let mut fields = Vec::with_capacity(nf);
            for _ in 0..nf {
                let flen = r.u16()? as usize;
                fields.push(
                    String::from_utf8(r.bytes(flen)?.to_vec())
                        .map_err(|_| Error::Corruption("meta: bad utf8 idx field".into()))?,
                );
            }
            let unique = r.u8()? != 0;
            let sparse = r.u8()? != 0;
            let root = PageId::new(r.u64()?);
            indexes.push(IndexDef {
                name,
                fields,
                unique,
                sparse,
                root,
            });
        }
        let dblen = r.u16()? as usize;
        let database = String::from_utf8(r.bytes(dblen)?.to_vec())
            .map_err(|_| Error::Corruption("meta: bad utf8 db name".into()))?;
        let clen = r.u16()? as usize;
        let name = String::from_utf8(r.bytes(clen)?.to_vec())
            .map_err(|_| Error::Corruption("meta: bad utf8 coll name".into()))?;
        // Text indexes are a trailing extension — absent in older metas.
        let mut text_indexes = Vec::new();
        if r.pos < buf.len() {
            let n = r.u16()? as usize;
            for _ in 0..n {
                let nlen = r.u16()? as usize;
                let tname = String::from_utf8(r.bytes(nlen)?.to_vec())
                    .map_err(|_| Error::Corruption("meta: bad utf8 text idx name".into()))?;
                let root = PageId::new(r.u64()?);
                let nf = r.u16()? as usize;
                let mut fields = Vec::with_capacity(nf);
                for _ in 0..nf {
                    let flen = r.u16()? as usize;
                    let field = String::from_utf8(r.bytes(flen)?.to_vec())
                        .map_err(|_| Error::Corruption("meta: bad utf8 text field".into()))?;
                    let weight = f64::from_bits(r.u64()?);
                    let analyzer_version = r.u32()?;
                    let clen = r.u32()? as usize;
                    let analyzer_cfg = r.bytes(clen)?.to_vec();
                    // Validate eagerly — a corrupt config must fail open, not
                    // surface mid-search.
                    Analyzer::decode_cfg(&analyzer_cfg)?;
                    fields.push(TextFieldDef {
                        field,
                        weight,
                        analyzer_cfg,
                        analyzer_version,
                    });
                }
                text_indexes.push(TextIndexDef {
                    name: tname,
                    fields,
                    root,
                });
            }
        }
        r.expect_end()?;
        Ok(CollectionMeta {
            id,
            database,
            name,
            pk_root,
            next_doc_id,
            dict,
            schema,
            indexes,
            text_indexes,
        })
    }
}

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn u8(&mut self) -> ndb_common::Result<u8> {
        Ok(*self
            .bytes(1)?
            .first()
            .ok_or_else(|| Error::Corruption("meta: eof".into()))?)
    }
    fn u16(&mut self) -> ndb_common::Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> ndb_common::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> ndb_common::Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self, n: usize) -> ndb_common::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(Error::Corruption("meta: truncated".into()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn expect_end(&self) -> ndb_common::Result<()> {
        if self.pos != self.buf.len() {
            return Err(Error::Corruption("meta: trailing bytes".into()));
        }
        Ok(())
    }
}
