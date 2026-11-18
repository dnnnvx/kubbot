//! Helpers for the Markdown the bot sends.

/// An event caps at 65 KB and holds the text twice, plain and as escaped HTML.
pub const MAX_LIST_BYTES: usize = 20_000;

/// A fence longer than any backtick run in the text, which can't close it early then.
pub fn code_block(text: &str) -> String {
    let longest = text
        .split(|c| c != '`')
        .map(str::len)
        .max()
        .unwrap_or_default();
    let fence = "`".repeat(longest.max(2) + 1);
    format!("{fence}\n{text}\n{fence}")
}

/// The first `max` bytes of `text` at most, cut on a character boundary.
pub fn head(text: &str, max: usize) -> &str {
    &text[..text.floor_char_boundary(max)]
}

/// The last `max` bytes of `text` at most, cut on a character boundary.
pub fn tail(text: &str, max: usize) -> &str {
    &text[text.ceil_char_boundary(text.len().saturating_sub(max))..]
}

/// `items` as a bulleted list of about `max` bytes, cut between items, saying how many were left out.
pub fn bullets(items: &[String], max: usize) -> String {
    let mut list = String::new();
    for (shown, item) in items.iter().enumerate() {
        if !list.is_empty() && list.len() + item.len() > max {
            list.push_str(&format!("\n\n…and {} more.", items.len() - shown));
            break;
        }
        if !list.is_empty() {
            list.push('\n');
        }
        list.push_str("- ");
        list.push_str(head(item, max));
    }
    list
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_outgrows_backticks_in_the_text() {
        assert_eq!(code_block("a"), "```\na\n```");
        assert_eq!(code_block("a ```` b"), "`````\na ```` b\n`````");
    }

    #[test]
    fn cuts_lists_between_items() {
        let items = ["one".to_owned(), "two".to_owned(), "three".to_owned()];
        assert_eq!(bullets(&items, 100), "- one\n- two\n- three");
        assert_eq!(bullets(&items, 10), "- one\n- two\n\n…and 1 more.");
        assert_eq!(
            bullets(&items[..1], 1),
            "- o",
            "the first item is cut, not dropped"
        );
    }

    #[test]
    fn cuts_on_character_boundaries() {
        // "é" is 2 bytes: a cut through its middle drops it whole.
        assert_eq!(head("aéb", 2), "a");
        assert_eq!(tail("aéb", 2), "b");
        assert_eq!(head("ab", 10), "ab");
        assert_eq!(tail("ab", 10), "ab");
    }
}
