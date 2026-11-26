//! English Porter stemmer — the reference 1980 algorithm, faithful to the
//! step order (1a..5b) as published in "An algorithm for suffix stripping".
//! Self-contained; terms < 3 chars pass through untouched.

/// Measure `m` = number of [C] ( VC )* [V] alternations — the algorithm's
/// central predicate. Computed over the stem prefix, not the whole word.
fn measure(w: &[u8]) -> usize {
    let mut m = 0;
    let mut i = 0;
    while i < w.len() && !is_vowel(w, i) {
        i += 1;
    }
    while i < w.len() {
        while i < w.len() && is_vowel(w, i) {
            i += 1;
        }
        if i < w.len() {
            m += 1;
        }
        while i < w.len() && !is_vowel(w, i) {
            i += 1;
        }
    }
    m
}

fn is_vowel(w: &[u8], i: usize) -> bool {
    match w[i] {
        b'a' | b'e' | b'i' | b'o' | b'u' => true,
        b'y' => i > 0 && !is_vowel(w, i - 1), // y is a vowel after a consonant
        _ => false,
    }
}

fn ends_with_cvc(w: &[u8]) -> bool {
    let n = w.len();
    n >= 3
        && !is_vowel(w, n - 3)
        && is_vowel(w, n - 2)
        && !is_vowel(w, n - 1)
        && !matches!(w[n - 1], b'w' | b'x' | b'y')
}

fn has_vowel(w: &[u8]) -> bool {
    (0..w.len()).any(|i| is_vowel(w, i))
}

fn double_consonant(w: &[u8]) -> bool {
    let n = w.len();
    n >= 2 && w[n - 1] == w[n - 2] && !is_vowel(w, n - 1)
}

/// Replace suffix `suf` with `rep` when stem has measure > threshold.
fn apply_gt(w: &mut Vec<u8>, suf: &str, rep: &str, threshold: usize) -> bool {
    if w.ends_with(suf.as_bytes()) {
        let stem = &w[..w.len() - suf.len()];
        if measure(stem) > threshold {
            w.truncate(w.len() - suf.len());
            w.extend_from_slice(rep.as_bytes());
        }
        true
    } else {
        false
    }
}

pub fn stem(word: &str) -> String {
    let mut w: Vec<u8> = word.as_bytes().to_vec();
    if w.len() <= 2 {
        return word.to_string();
    }

    // ---- Step 1a: plurals
    if w.ends_with(b"sses") || w.ends_with(b"ies") {
        w.truncate(w.len() - 2);
    } else if w.ends_with(b"ss") {
    } else if w.ends_with(b"s") {
        w.truncate(w.len() - 1);
    }

    // ---- Step 1b: -ed / -ing
    let mut stemmed_1b = false;
    for suf in [b"eed".as_slice(), b"ed".as_slice(), b"ing".as_slice()] {
        if w.ends_with(suf) {
            let stem = &w[..w.len() - suf.len()];
            if suf == b"eed" {
                if measure(stem) > 0 {
                    w.truncate(w.len() - 1); // eed → ee
                }
            } else if has_vowel(stem) {
                w.truncate(w.len() - suf.len());
                stemmed_1b = true;
            }
            break;
        }
    }
    if stemmed_1b {
        if w.ends_with(b"at") || w.ends_with(b"bl") || w.ends_with(b"iz") {
            w.push(b'e');
        } else if double_consonant(&w) && !matches!(*w.last().unwrap(), b'l' | b's' | b'z') {
            w.truncate(w.len() - 1);
        } else if measure(&w) == 1 && ends_with_cvc(&w) {
            w.push(b'e');
        }
    }

    // ---- Step 1c: terminal y → i when preceded by a vowel
    if w.ends_with(b"y") && w.len() > 1 && has_vowel(&w[..w.len() - 1]) {
        *w.last_mut().unwrap() = b'i';
    }

    // ---- Steps 2..4: suffix substitution/removal by measure
    const S2: &[(&str, &str)] = &[
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("izer", "ize"),
        ("abli", "able"),
        ("alli", "al"),
        ("entli", "ent"),
        ("eli", "e"),
        ("ousli", "ous"),
        ("ization", "ize"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("alism", "al"),
        ("iveness", "ive"),
        ("fulness", "ful"),
        ("ousness", "ous"),
        ("aliti", "al"),
        ("iviti", "ive"),
        ("biliti", "ble"),
    ];
    for (suf, rep) in S2 {
        if w.ends_with(suf.as_bytes()) {
            apply_gt(&mut w, suf, rep, 0);
            break;
        }
    }

    const S3: &[(&str, &str)] = &[
        ("icate", "ic"),
        ("ative", ""),
        ("alize", "al"),
        ("iciti", "ic"),
        ("ical", "ic"),
        ("ful", ""),
        ("ness", ""),
    ];
    for (suf, rep) in S3 {
        if w.ends_with(suf.as_bytes()) {
            apply_gt(&mut w, suf, rep, 0);
            break;
        }
    }

    const S4: &[&str] = &[
        "ement", "ance", "ence", "able", "ible", "ment", "ent", "al", "ent", "ism", "ate", "iti",
        "ous", "ive", "ize", "ant", "er", "ic", "ou", "ion",
    ];
    for suf in S4 {
        if w.ends_with(suf.as_bytes()) {
            if *suf == "ion" {
                // ion requires preceding s or t
                let stem = &w[..w.len() - 3];
                if measure(stem) > 1 && stem.last().is_some_and(|c| *c == b's' || *c == b't') {
                    w.truncate(w.len() - 3);
                }
            } else {
                let stem = &w[..w.len() - suf.len()];
                if measure(stem) > 1 {
                    w.truncate(w.len() - suf.len());
                }
            }
            break;
        }
    }

    // ---- Step 5a: trailing e
    if w.ends_with(b"e") {
        let stem = &w[..w.len() - 1];
        let m = measure(stem);
        if m > 1 || (m == 1 && !ends_with_cvc(stem)) {
            w.truncate(w.len() - 1);
        }
    }
    // ---- Step 5b: double l
    if measure(&w) > 1 && w.ends_with(b"ll") {
        w.truncate(w.len() - 1);
    }

    String::from_utf8(w).unwrap_or_else(|_| word.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_examples() {
        let cases = [
            ("caresses", "caress"),
            ("ponies", "poni"),
            ("ties", "ti"),
            ("caress", "caress"),
            ("cats", "cat"),
            ("feed", "feed"),
            ("agreed", "agre"),
            ("plastered", "plaster"),
            ("bled", "bled"),
            ("motoring", "motor"),
            ("sing", "sing"),
            ("conflated", "conflat"),
            ("troubled", "troubl"),
            ("sized", "size"),
            ("hopping", "hop"),
            ("tanned", "tan"),
            ("falling", "fall"),
            ("hissing", "hiss"),
            ("fizzed", "fizz"),
            ("failing", "fail"),
            ("filing", "file"),
            ("happy", "happi"),
            ("sky", "sky"),
            ("relational", "relat"),
            ("conditional", "condit"),
            ("rational", "ration"),
            ("valenci", "valenc"),
            ("hesitanci", "hesit"),
            ("digitizer", "digit"),
            ("radicalli", "radic"),
            ("differentli", "differ"),
            ("vileli", "vile"),
            ("analogousli", "analog"),
            ("vietnamization", "vietnam"),
            ("predication", "predic"),
            ("operator", "oper"),
            ("feudalism", "feudal"),
            ("decisiveness", "decis"),
            ("hopefulness", "hope"),
            ("callousness", "callous"),
            ("formaliti", "formal"),
            ("sensitiviti", "sensit"),
            ("sensibiliti", "sensibl"),
            ("triplicate", "triplic"),
            ("formative", "form"),
            ("formalize", "formal"),
            ("electriciti", "electr"),
            ("electrical", "electr"),
            ("hopeful", "hope"),
            ("goodness", "good"),
            ("revival", "reviv"),
            ("allowance", "allow"),
            ("inference", "infer"),
            ("airliner", "airlin"),
            ("gyroscopic", "gyroscop"),
            ("adjustable", "adjust"),
            ("defensible", "defens"),
            ("irritant", "irrit"),
            ("replacement", "replac"),
            ("adjustment", "adjust"),
            ("dependent", "depend"),
            ("adoption", "adopt"),
            ("homologou", "homolog"),
            ("communism", "commun"),
            ("activate", "activ"),
            ("angulariti", "angular"),
            ("homologous", "homolog"),
            ("effective", "effect"),
            ("bowdlerize", "bowdler"),
            ("probate", "probat"),
            ("rate", "rate"),
            ("cease", "ceas"),
            ("controll", "control"),
            ("roll", "roll"),
            ("running", "run"),
            ("jumps", "jump"),
            ("dogs", "dog"),
        ];
        for (input, want) in cases {
            assert_eq!(stem(input), want, "stem({input})");
        }
    }
}
