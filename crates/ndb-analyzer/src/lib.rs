//! Text analysis pipeline (spec §9.2).
//!
//! ```text
//! normalize (Unicode-ish fold) → tokenize → case fold → stop-word filter
//!   → stem (Porter, English) → synonym expansion
//! ```
//!
//! An [`Analyzer`] is built from a versioned [`AnalyzerConfig`] — the
//! version is stored on the index definition, and bumping it must trigger
//! an index rebuild rather than silently mixing tokenisation rules
//! (spec §9.2). Per-field configs are held by the collection's text-index
//! definition.

mod porter;

use ndb_common::error::Error;
use ndb_tokenizer::{Token, tokenize};
use std::collections::{BTreeMap, BTreeSet};

/// Versioned, per-field analysis configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzerConfig {
    /// Bump on any semantic change; index defs pin a version (spec §9.2).
    pub version: u32,
    /// Fold accents to ASCII (café → cafe).
    pub fold_accents: bool,
    /// Case-fold to lowercase.
    pub lowercase: bool,
    /// Drop these terms (post-fold).
    pub stopwords: BTreeSet<String>,
    /// Porter-stem terms when true.
    pub stem: bool,
    /// term → extra synonyms emitted at the same position.
    pub synonyms: BTreeMap<String, Vec<String>>,
}

impl Default for AnalyzerConfig {
    /// English-language standard analyzer: fold, lowercase, stop-words,
    /// stemming — no synonyms.
    fn default() -> Self {
        Self {
            version: 1,
            fold_accents: true,
            lowercase: true,
            stopwords: default_stopwords(),
            stem: true,
            synonyms: BTreeMap::new(),
        }
    }
}

/// A self-contained analysis pipeline.
pub struct Analyzer {
    cfg: AnalyzerConfig,
}

impl Analyzer {
    pub fn new(cfg: AnalyzerConfig) -> Self {
        Self { cfg }
    }

    /// The versioned configuration this analyzer was built from.
    pub fn config(&self) -> &AnalyzerConfig {
        &self.cfg
    }

    /// Codec for storing the config in index metadata.
    pub fn encode_cfg(cfg: &AnalyzerConfig) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&cfg.version.to_le_bytes());
        out.push(cfg.fold_accents as u8);
        out.push(cfg.lowercase as u8);
        out.push(cfg.stem as u8);
        out.extend_from_slice(&(cfg.stopwords.len() as u32).to_le_bytes());
        for w in &cfg.stopwords {
            out.extend_from_slice(&(w.len() as u16).to_le_bytes());
            out.extend_from_slice(w.as_bytes());
        }
        out.extend_from_slice(&(cfg.synonyms.len() as u32).to_le_bytes());
        for (k, vs) in &cfg.synonyms {
            out.extend_from_slice(&(k.len() as u16).to_le_bytes());
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(&(vs.len() as u16).to_le_bytes());
            for v in vs {
                out.extend_from_slice(&(v.len() as u16).to_le_bytes());
                out.extend_from_slice(v.as_bytes());
            }
        }
        out
    }

    pub fn decode_cfg(buf: &[u8]) -> ndb_common::Result<AnalyzerConfig> {
        let mut r = Reader { buf, pos: 0 };
        let version = r.u32()?;
        let fold_accents = r.u8()? != 0;
        let lowercase = r.u8()? != 0;
        let stem = r.u8()? != 0;
        let nstop = r.u32()? as usize;
        let mut stopwords = BTreeSet::new();
        for _ in 0..nstop {
            stopwords.insert(r.string()?);
        }
        let nsyn = r.u32()? as usize;
        let mut synonyms = BTreeMap::new();
        for _ in 0..nsyn {
            let k = r.string()?;
            let nv = r.u16()? as usize;
            let mut vs = Vec::with_capacity(nv);
            for _ in 0..nv {
                vs.push(r.string()?);
            }
            synonyms.insert(k, vs);
        }
        if r.pos != buf.len() {
            return Err(Error::Corruption("analyzer cfg: trailing".into()));
        }
        Ok(AnalyzerConfig {
            version,
            fold_accents,
            lowercase,
            stopwords,
            stem,
            synonyms,
        })
    }

    /// Full pipeline: raw text → final tokens. Original positions are kept
    /// even for filtered tokens — synonym expansions share the source
    /// position so phrase queries see them as alternatives, not sequence.
    pub fn analyze(&self, text: &str) -> Vec<Token> {
        let mut out = Vec::new();
        for tok in tokenize(text) {
            let mut term = tok.text.clone();
            if self.cfg.fold_accents {
                term = fold_accents(&term);
            }
            if self.cfg.lowercase {
                term = term.to_lowercase();
            }
            if self.cfg.stopwords.contains(&term) {
                continue;
            }
            if self.cfg.stem {
                term = porter::stem(&term);
            }
            out.push(Token {
                text: term.clone(),
                position: tok.position,
                start: tok.start,
                end: tok.end,
            });
            if let Some(syns) = self.cfg.synonyms.get(&term) {
                for s in syns {
                    let mut st = s.clone();
                    if self.cfg.stem {
                        st = porter::stem(&st);
                    }
                    out.push(Token {
                        text: st,
                        position: tok.position,
                        start: tok.start,
                        end: tok.end,
                    });
                }
            }
        }
        out
    }
}

/// Accent/diacritic folding over common Latin ranges (no external
/// normalisation tables — a pragmatic map covering Latin-1 + common marks).
fn fold_accents(s: &str) -> String {
    s.chars()
        .flat_map(|c| {
            match c {
                'à'..='å' | 'ā' | 'ă' | 'ą' => vec!['a'],
                'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => vec!['c'],
                'ď' | 'đ' => vec!['d'],
                'è'..='ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => vec!['e'],
                'ĝ' | 'ğ' | 'ġ' | 'ģ' => vec!['g'],
                'ĥ' | 'ħ' => vec!['h'],
                'ì'..='ï' | 'ĩ' | 'ī' | 'ĭ' | 'į' | 'ı' => vec!['i'],
                'ĵ' => vec!['j'],
                'ķ' => vec!['k'],
                'ĺ' | 'ļ' | 'ľ' | 'ŀ' | 'ł' => vec!['l'],
                'ñ' | 'ń' | 'ņ' | 'ň' | 'ŋ' => vec!['n'],
                'ò'..='ö' | 'ō' | 'ŏ' | 'ő' | 'ø' => vec!['o'],
                'ŕ' | 'ŗ' | 'ř' => vec!['r'],
                'ś' | 'ŝ' | 'ş' | 'š' => vec!['s'],
                'ţ' | 'ť' | 'ŧ' => vec!['t'],
                'ù'..='ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => vec!['u'],
                'ŵ' => vec!['w'],
                'ý' | 'ÿ' => vec!['y'],
                'ź' | 'ż' | 'ž' => vec!['z'],
                'œ' => vec!['o', 'e'],
                'æ' => vec!['a', 'e'],
                'ß' => vec!['s', 's'],
                other => vec![other],
            }
            .into_iter()
        })
        .collect()
}

/// A small standard English stop-word list (Lucene-style defaults).
fn default_stopwords() -> BTreeSet<String> {
    [
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
        "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn u8(&mut self) -> ndb_common::Result<u8> {
        let s = self.bytes(1)?;
        Ok(s[0])
    }
    fn u16(&mut self) -> ndb_common::Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> ndb_common::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn string(&mut self) -> ndb_common::Result<String> {
        let n = self.u16()? as usize;
        String::from_utf8(self.bytes(n)?.to_vec())
            .map_err(|_| Error::Corruption("analyzer cfg: bad utf8".into()))
    }
    fn bytes(&mut self, n: usize) -> ndb_common::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(Error::Corruption("analyzer cfg: truncated".into()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_folds_stems_filters() {
        let a = Analyzer::new(AnalyzerConfig::default());
        let toks = a.analyze("The RUNNING Café's dogs");
        let texts: Vec<&str> = toks.iter().map(|t| t.text.as_str()).collect();
        // "the" is a stop-word; running→run; café's→café'→cafe'... hmm
        assert!(texts.contains(&"run"));
        assert!(texts.contains(&"cafe's") || texts.contains(&"cafe"));
        assert!(texts.contains(&"dog"));
        assert!(!texts.contains(&"the"));
    }

    #[test]
    fn synonyms_share_position() {
        let mut cfg = AnalyzerConfig {
            stem: false,
            ..AnalyzerConfig::default()
        };
        cfg.synonyms
            .insert("auto".into(), vec!["car".into(), "vehicle".into()]);
        let a = Analyzer::new(cfg);
        let toks = a.analyze("auto fast");
        let texts: Vec<(&str, u32)> = toks.iter().map(|t| (t.text.as_str(), t.position)).collect();
        assert!(texts.contains(&("car", 0)));
        assert!(texts.contains(&("vehicle", 0)));
        assert!(texts.contains(&("fast", 1)));
    }

    #[test]
    fn cfg_roundtrips() {
        let cfg = AnalyzerConfig::default();
        let got = Analyzer::decode_cfg(&Analyzer::encode_cfg(&cfg)).unwrap();
        assert_eq!(got, cfg);
    }
}
