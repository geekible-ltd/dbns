//! Tokenisation (spec §9.2 — analysis pipeline input).
//!
//! Splits text into [`Token`]s carrying the term text, the stream position
//! (for phrase queries — gaps from removed tokens are preserved), and byte
//! offsets (for highlighting later).
//!
//! The tokenizer only *splits* — normalisation, case folding, stemming
//! etc. are the analyser's job (spec §9.2 separates the stages).

/// A lexical unit with positional metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    /// Raw term text as written (pre-normalisation).
    pub text: String,
    /// Token-stream position — increments per emitted token, including
    /// positions the analyser may later filter, so phrase slop stays
    /// accurate.
    pub position: u32,
    /// Byte offsets into the source text.
    pub start: usize,
    pub end: usize,
}

/// Splits `text` into tokens on non-alphanumeric boundaries.
///
/// Words are maximal runs of Unicode alphanumeric characters. `_` and `'` inside a
/// run are kept (identifiers and contractions stay whole).
pub fn tokenize(text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut pos = 0u32;
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = text[i..].chars().next().unwrap();
        if is_word_char(ch) {
            let start = i;
            let mut end = i + ch.len_utf8();
            while end < bytes.len() {
                let c = text[end..].chars().next().unwrap();
                if is_word_char(c) || (is_joiner(c) && has_word_after(&text[end..])) {
                    end += c.len_utf8();
                } else {
                    break;
                }
            }
            // Possessive 's is stripped (contraction 't/'re/etc. stays).
            let end = if text[start..end].ends_with("'s") || text[start..end].ends_with("'S") {
                end - 2
            } else {
                end
            };
            out.push(Token {
                text: text[start..end].to_string(),
                position: pos,
                start,
                end,
            });
            pos += 1;
            i = end;
        } else {
            i += ch.len_utf8();
        }
    }
    out
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric()
}

fn is_joiner(c: char) -> bool {
    c == '_' || c == '\''
}

/// `_`/`'` only join when followed by another word character, so trailing
/// punctuation never glues onto a token.
fn has_word_after(rest: &str) -> bool {
    rest.chars().nth(1).is_some_and(is_word_char)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_punctuation_and_space() {
        let toks = tokenize("the Quick, brown-fox jumps");
        let texts: Vec<&str> = toks.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, vec!["the", "Quick", "brown", "fox", "jumps"]);
        assert_eq!(toks[4].position, 4);
    }

    #[test]
    fn keeps_underscores_and_contractions() {
        let toks = tokenize("user_id don't end_");
        let texts: Vec<&str> = toks.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, vec!["user_id", "don't", "end"]);
    }

    #[test]
    fn unicode_alphanumerics() {
        let toks = tokenize("café naïve ");
        assert_eq!(toks.len(), 2);
        assert_eq!(toks[0].text, "café");
        assert_eq!(toks[1].text, "naïve");
    }

    #[test]
    fn byte_offsets_point_back() {
        let src = "ab cd";
        let toks = tokenize(src);
        assert_eq!(&src[toks[1].start..toks[1].end], "cd");
    }
}
