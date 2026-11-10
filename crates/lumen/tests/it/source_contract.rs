//! Active Rust source tokens for metadata contracts. This does not execute the
//! source. Literal contents, numeric values, punctuation and order stay exact.

use std::ops::Range;

pub(super) const RESTART_RECORD_ASSIGNMENT: &str = r#"let record = if readiness.readiness_failure.is_some() {
    emit_restart_phase_diagnostics_with_readiness(diagnostics, readiness, phase, outcome,)
} else {
    emit_restart_phase_diagnostics(diagnostics, phase, outcome)
};"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Token<'a> {
    pub(super) text: &'a str,
    span: Range<usize>,
}

fn quoted_end(source: &str, quote: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut cursor = quote + 1;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' => cursor += 2,
            b'"' => return Some(cursor + 1),
            _ => cursor += 1,
        }
    }
    None
}

fn raw_end(source: &str, start: usize) -> Option<Result<usize, &'static str>> {
    let bytes = source.as_bytes();
    let raw = if bytes.get(start) == Some(&b'r') {
        start
    } else if matches!(bytes.get(start), Some(b'b' | b'c')) && bytes.get(start + 1) == Some(&b'r') {
        start + 1
    } else {
        return None;
    };
    let mut quote = raw + 1;
    while bytes.get(quote) == Some(&b'#') {
        quote += 1;
    }
    if bytes.get(quote) != Some(&b'"') {
        return None;
    }
    let terminator = format!("\"{}", "#".repeat(quote - raw - 1));
    Some(
        source[quote + 1..]
            .find(&terminator)
            .map(|offset| quote + 1 + offset + terminator.len())
            .ok_or("unterminated raw literal"),
    )
}

/// Comments are absent. Every literal is one complete token, so a string that
/// looks like code or a comment cannot create an executable requirement.
pub(super) fn tokens(source: &str) -> Result<Vec<Token<'_>>, &'static str> {
    let bytes = source.as_bytes();
    let mut result = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let character = source[cursor..].chars().next().ok_or("source boundary")?;
        if character.is_whitespace() {
            cursor += character.len_utf8();
            continue;
        }
        if source[cursor..].starts_with("//") {
            cursor = source[cursor..]
                .find('\n')
                .map_or(bytes.len(), |offset| cursor + offset);
            continue;
        }
        if source[cursor..].starts_with("/*") {
            cursor += 2;
            let mut depth = 1usize;
            while cursor < bytes.len() && depth != 0 {
                if source[cursor..].starts_with("/*") {
                    depth = depth.checked_add(1).ok_or("comment depth overflow")?;
                    cursor += 2;
                } else if source[cursor..].starts_with("*/") {
                    depth -= 1;
                    cursor += 2;
                } else {
                    cursor += source[cursor..]
                        .chars()
                        .next()
                        .ok_or("comment boundary")?
                        .len_utf8();
                }
            }
            if depth != 0 {
                return Err("unterminated block comment");
            }
            continue;
        }
        let start = cursor;
        if let Some(end) = raw_end(source, cursor) {
            cursor = end?;
        } else if bytes[cursor] == b'"' {
            cursor = quoted_end(source, cursor).ok_or("unterminated literal")?;
        } else if matches!(bytes[cursor], b'b' | b'c') && bytes.get(cursor + 1) == Some(&b'"') {
            cursor = quoted_end(source, cursor + 1).ok_or("unterminated literal")?;
        } else if character.is_alphanumeric() || character == '_' {
            cursor += character.len_utf8();
            while cursor < bytes.len() {
                let next = source[cursor..].chars().next().ok_or("word boundary")?;
                if !next.is_alphanumeric() && next != '_' {
                    break;
                }
                cursor += next.len_utf8();
            }
        } else if bytes[cursor] == b'\'' {
            // Keep a character literal whole; an identifier lifetime has no
            // immediate closing quote and retains its own punctuation token.
            let mut end = cursor + 1;
            if bytes.get(end) == Some(&b'\\') {
                end += 1;
                match bytes.get(end) {
                    Some(b'x') => end += 3,
                    Some(b'u') if bytes.get(end + 1) == Some(&b'{') => {
                        end = source[end + 2..]
                            .find('}')
                            .map(|offset| end + 3 + offset)
                            .ok_or("unterminated character escape")?;
                    }
                    Some(_) => end += 1,
                    None => return Err("unterminated character escape"),
                }
            } else if let Some(next) = source[end..].chars().next() {
                end += next.len_utf8();
            }
            cursor = if bytes.get(end) == Some(&b'\'') {
                end + 1
            } else {
                cursor + 1
            };
        } else {
            cursor += character.len_utf8();
        }
        result.push(Token {
            text: &source[start..cursor],
            span: start..cursor,
        });
    }
    Ok(result)
}

fn occurrences(source: &str, expected: &str, boundary: bool) -> Option<Vec<Range<usize>>> {
    let source = tokens(source).ok()?;
    let expected = tokens(expected).ok()?;
    if expected.is_empty() {
        return None;
    }
    Some(
        source
            .windows(expected.len())
            .enumerate()
            .filter(|(index, _)| {
                !boundary
                    || *index == 0
                    || matches!(source[index - 1].text, "{" | "}" | ";" | "," | "]")
            })
            .map(|(_, window)| window)
            .filter(|window| {
                window
                    .iter()
                    .zip(&expected)
                    .all(|(left, right)| left.text == right.text)
            })
            .map(|window| window[0].span.start..window[window.len() - 1].span.end)
            .collect(),
    )
}

pub(super) fn contains(source: &str, expected: &str) -> bool {
    occurrences(source, expected, false).is_some_and(|found| !found.is_empty())
}

pub(super) fn count(source: &str, expected: &str) -> usize {
    occurrences(source, expected, false).map_or(0, |found| found.len())
}

pub(super) fn literal_fragment(source: &str, expected: &str) -> bool {
    !expected.is_empty()
        && tokens(source).is_ok_and(|tokens| {
            tokens.iter().any(|token| {
                (token.text.starts_with('"')
                    || token.text.starts_with("b\"")
                    || token.text.starts_with("c\"")
                    || token.text.starts_with("r\"")
                    || token.text.starts_with("r#")
                    || token.text.starts_with("br\"")
                    || token.text.starts_with("br#")
                    || token.text.starts_with("cr\"")
                    || token.text.starts_with("cr#"))
                    && token.text.contains(expected)
            })
        })
}

pub(super) fn spans(source: &str, expected: &str) -> Option<Vec<Range<usize>>> {
    occurrences(source, expected, false)
}

pub(super) fn same_tokens(left: &str, right: &str) -> bool {
    match (tokens(left), tokens(right)) {
        (Ok(left), Ok(right)) if !left.is_empty() => {
            left.iter().map(|t| t.text).eq(right.iter().map(|t| t.text))
        }
        _ => false,
    }
}

/// The exact signature, including visibility/async/parameters, must be unique.
/// Nested blocks and braces inside literals cannot change the body boundary.
pub(super) fn body<'a>(source: &'a str, signature: &str) -> Option<&'a str> {
    let signatures = occurrences(source, signature, true)?;
    let [signature] = signatures.as_slice() else {
        return None;
    };
    let all = tokens(source).ok()?;
    let opening = all
        .iter()
        .position(|token| token.span.end == signature.end && token.text == "{")?;
    let mut depth = 1usize;
    for token in &all[opening + 1..] {
        match token.text {
            "{" => depth = depth.checked_add(1)?,
            "}" => {
                depth -= 1;
                if depth == 0 {
                    return source.get(signature.end..token.span.start);
                }
            }
            _ => {}
        }
    }
    None
}

pub(super) fn region<'a>(source: &'a str, begin: &str, end: &str) -> Option<&'a str> {
    let begins = occurrences(source, begin, true)?;
    let ends = occurrences(source, end, true)?;
    match (begins.as_slice(), ends.as_slice()) {
        ([begin], [end]) if begin.end <= end.start => Some(&source[begin.end..end.start]),
        _ => None,
    }
}

pub(super) fn test_inventory(source: &str) -> Option<Vec<(String, bool)>> {
    let tokens = tokens(source).ok()?;
    let mut test = false;
    let mut ignored = false;
    let mut inventory = Vec::new();
    let mut cursor = 0;
    while cursor < tokens.len() {
        if tokens[cursor].text == "#" && tokens.get(cursor + 1)?.text == "[" {
            let start = cursor + 2;
            cursor = start;
            let mut depth = 1;
            while cursor < tokens.len() && depth != 0 {
                match tokens[cursor].text {
                    "[" => depth += 1,
                    "]" => depth -= 1,
                    _ => {}
                }
                cursor += 1;
            }
            if depth != 0 {
                return None;
            }
            test |= tokens.get(start)?.text == "test";
            ignored |= tokens.get(start)?.text == "ignore";
            continue;
        }
        if tokens[cursor].text == "fn" {
            if test {
                inventory.push((tokens.get(cursor + 1)?.text.to_owned(), ignored));
            }
            test = false;
            ignored = false;
        } else if matches!(
            tokens[cursor].text,
            "struct" | "enum" | "const" | "mod" | "impl"
        ) {
            test = false;
            ignored = false;
        }
        cursor += 1;
    }
    Some(inventory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multiline_statements_keep_exact_values_and_order() {
        let source = "let record =\n emit(diagnostics, phase, outcome);";
        assert!(contains(
            source,
            "let record = emit(diagnostics, phase, outcome);"
        ));
        assert!(!contains(
            source,
            "let record = emit(diagnostics, outcome, phase);"
        ));
        assert!(!contains(
            "const LIMIT: u64 = 31;",
            "const LIMIT: u64 = 30;"
        ));
        assert!(!contains("(value,)", "(value)"));
    }

    #[test]
    fn strings_raw_strings_and_nested_comments_cannot_supply_active_code() {
        let source = r###"let url = "http://local/*"; let raw = br#"#[test] fn fake() {}"#;
        /* #[test] fn hidden() {} /* nested */ */
        #[test] fn actual() {}"###;
        assert_eq!(test_inventory(source), Some(vec![("actual".into(), false)]));
        assert!(!contains(source, "fn fake() {}"));
        assert!(!contains(source, "fn hidden() {}"));
        assert!(contains(source, "let url = \"http://local/*\";"));
    }

    #[test]
    fn quoted_comment_markers_do_not_hide_later_tests() {
        let source = "fn helper() { let prefix = b\"/*\"; }\n#[test]\nfn required() {}";
        assert_eq!(
            test_inventory(source),
            Some(vec![("required".into(), false)])
        );
        assert_eq!(test_inventory("/* unfinished"), None);
        assert!(!contains("let x = r#\"unfinished", "x"));
    }

    #[test]
    fn ignored_duplicate_and_removed_names_remain_visible() {
        let source = "#[test] #[ignore = \"external\"] fn gate() {} #[test] fn gate() {}";
        assert_eq!(
            test_inventory(source),
            Some(vec![("gate".into(), true), ("gate".into(), false)])
        );
        assert_eq!(test_inventory("fn gate() {}"), Some(vec![]));
    }

    #[test]
    fn region_bounds_require_unique_active_tokens() {
        let source = "begin();\nrequired();\nend();";
        assert!(contains(
            region(source, "begin();", "end();").unwrap(),
            "required();"
        ));
        assert!(region("begin(); begin(); end();", "begin();", "end();").is_none());
        assert!(region("\"begin();\"; end();", "begin();", "end();").is_none());
        assert_eq!(count("step(); // step();\nstep();", "step();"), 2);
        assert!(region(
            "match x { \"a\" | \"b\" => { required(); }, \"c\" => {} }",
            "\"b\" => {",
            "\"c\" => {"
        )
        .is_none());
    }

    #[test]
    fn characters_and_lifetimes_keep_their_own_boundaries() {
        let source = r#"fn read<'a>(x: &'a str) { let quote = '\''; let slash = '/'; }"#;
        assert!(contains(source, "let quote = '\\'';"));
        assert!(contains(source, "let slash = '/';"));
        assert!(contains(source, "fn read<'a>(x: &'a str)"));
        assert!(!contains(source, "let slash = '*';"));
    }

    #[test]
    fn exact_signature_and_body_keep_visibility_and_nested_literals() {
        let source = "async fn work(x: &str) { if true { emit(\"}\"); } }";
        assert!(contains(
            body(source, "async fn work(x: &str) {").unwrap(),
            "emit(\"}\");"
        ));
        assert!(body(source, "fn work(x: &str) {").is_none());
        assert!(body("pub async fn work(x: &str) {}", "async fn work(x: &str) {").is_none());
        assert!(!same_tokens("(item,)", "(item)"));
        assert!(same_tokens(
            "step(\"exact text\");",
            "step(\"exact text\") ;"
        ));
        assert_eq!(spans("step(); step();", "step();").unwrap().len(), 2);
    }

    #[test]
    fn literal_fields_never_stand_in_for_code_requirements() {
        assert!(literal_fragment(
            "emit(\"readyz_status={status}\");",
            "readyz_status="
        ));
        assert!(!literal_fragment("// \"readyz_status=\"", "readyz_status="));
        assert!(!literal_fragment("readyz_status = 200;", "readyz_status="));
        assert!(!contains(
            "let decoy = \"const LIMIT: u64 = 30;\";",
            "const LIMIT: u64 = 30;"
        ));
        assert!(contains(
            RESTART_RECORD_ASSIGNMENT,
            RESTART_RECORD_ASSIGNMENT
        ));
        assert!(!contains(
            RESTART_RECORD_ASSIGNMENT,
            "let record = emit_restart_phase_diagnostics(diagnostics, phase, outcome);"
        ));
    }
}
