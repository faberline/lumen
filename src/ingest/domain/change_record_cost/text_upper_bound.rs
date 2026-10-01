//! Allocation-free upper bounds on the terms an analyzer can produce from a
//! text value.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnalyzerKind {
    WhitespaceLower,
    Jieba,
    Ngram,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TextUpperBound {
    /// Repetitions are possible distinct terms. No token set is needed.
    pub terms: usize,
    pub total_utf8_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NormalizeError {
    Overflow,
    /// Caller must retain the record and obtain schema/live coverage. Never discard.
    ContextMissing,
}

/// This allocates no normalized tokens.  It mirrors the public analyzer's
/// lowercasing walk and counts scalar windows with checked arithmetic.
pub fn text_upper_bound(
    input: &str,
    analyzer: AnalyzerKind,
    ngram_min: usize,
    ngram_max: usize,
) -> Result<TextUpperBound, NormalizeError> {
    match analyzer {
        AnalyzerKind::Ngram => {
            ngram_upper_bound(lowered_non_whitespace_scalars(input)?, ngram_min, ngram_max)
        }
        AnalyzerKind::WhitespaceLower => whitespace_lower_upper_bound(input),
        AnalyzerKind::Jieba => jieba_upper_bound(input),
    }
}

/// Exactly count valid windows after the public n-gram normalizer removes
/// whitespace and expands `char::to_lowercase`.  A scalar emits at most four
/// UTF-8 bytes, so this is a byte upper bound without a token Vec or a window
/// allocation.
pub fn ngram_upper_bound(
    scalars: usize,
    ngram_min: usize,
    ngram_max: usize,
) -> Result<TextUpperBound, NormalizeError> {
    if ngram_min == 0 || ngram_min > ngram_max {
        return Ok(TextUpperBound::default());
    }
    let mut terms = 0usize;
    let mut bytes = 0usize;
    for width in ngram_min..=ngram_max {
        let Some(windows) = scalars.checked_sub(width).and_then(|n| n.checked_add(1)) else {
            continue;
        };
        terms = add(terms, windows)?;
        bytes = add(bytes, mul(mul(windows, width)?, 4)?)?;
    }
    Ok(TextUpperBound {
        terms,
        total_utf8_bytes: bytes,
    })
}

fn lowered_non_whitespace_scalars(input: &str) -> Result<usize, NormalizeError> {
    let mut count = 0usize;
    for character in input.chars().filter(|character| !character.is_whitespace()) {
        for _ in character.to_lowercase() {
            count = add(count, 1)?;
        }
    }
    Ok(count)
}

fn lowered_utf8_bytes(input: &str) -> Result<usize, NormalizeError> {
    let mut bytes = 0usize;
    for character in input.chars() {
        for lowered in character.to_lowercase() {
            bytes = add(bytes, lowered.len_utf8())?;
        }
    }
    Ok(bytes)
}

/// Match `index_text::for_whitespace_lower_cow` without creating output
/// strings.  Punctuation-only words produce no token.
fn whitespace_lower_upper_bound(mut text: &str) -> Result<TextUpperBound, NormalizeError> {
    let mut terms = 0usize;
    let mut bytes = 0usize;
    while !text.is_empty() {
        let trimmed = text.trim_start();
        if trimmed.is_empty() {
            break;
        }
        text = trimmed;
        let end = text.find(char::is_whitespace).unwrap_or(text.len());
        let raw = &text[..end];
        text = &text[end..];
        let token = raw.trim_matches(|character: char| !character.is_alphanumeric());
        if !token.is_empty() {
            terms = add(terms, 1)?;
            bytes = add(bytes, lowered_utf8_bytes(token)?)?;
        }
    }
    Ok(TextUpperBound {
        terms,
        total_utf8_bytes: bytes,
    })
}

/// With the `jieba` feature, cuts are non-overlapping lowered substrings.  The
/// fallback emits CJK bigrams, where an input scalar can occur in two output
/// windows.  Count every non-whitespace lowered scalar as a possible term and
/// charge at most two four-byte appearances.  This is conservative for both
/// feature modes without constructing the fallback's `Vec<char>`.
fn jieba_upper_bound(input: &str) -> Result<TextUpperBound, NormalizeError> {
    let scalars = lowered_non_whitespace_scalars(input)?;
    Ok(TextUpperBound {
        terms: scalars,
        total_utf8_bytes: mul(scalars, 8)?,
    })
}

pub(super) fn add(left: usize, right: usize) -> Result<usize, NormalizeError> {
    left.checked_add(right).ok_or(NormalizeError::Overflow)
}

fn mul(left: usize, right: usize) -> Result<usize, NormalizeError> {
    left.checked_mul(right).ok_or(NormalizeError::Overflow)
}
