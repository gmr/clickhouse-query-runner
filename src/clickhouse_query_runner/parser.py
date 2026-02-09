"""SQL file parsing for ClickHouse query runner."""

from __future__ import annotations

import hashlib
import logging

LOGGER = logging.getLogger(__name__)


def parse_sql_file(file_path: str) -> list[tuple[str, str]]:
    """Parse a SQL file into individual statements with their hashes.

    Splits on semicolons while respecting string literals and comments.

    Returns:
        List of (query_hash, query_text) tuples in file order.
    """
    with open(file_path, encoding='utf-8') as fh:
        content = fh.read()

    statements = _split_statements(content)
    result = []
    for stmt in statements:
        stripped = stmt.strip()
        if not stripped:
            continue
        query_hash = hashlib.sha256(stripped.encode()).hexdigest()
        result.append((query_hash, stripped))

    LOGGER.debug('Parsed %d queries from %s', len(result), file_path)
    return result


def _split_statements(content: str) -> list[str]:
    """Split SQL content into statements on semicolons.

    Handles single-quoted strings, double-quoted identifiers,
    single-line comments (--), and block comments.
    """
    statements: list[str] = []
    current: list[str] = []
    i = 0
    length = len(content)

    while i < length:
        char = content[i]

        # Single-line comment
        if char == '-' and i + 1 < length and content[i + 1] == '-':
            end = content.find('\n', i)
            if end == -1:
                current.append(content[i:])
                break
            current.append(content[i : end + 1])
            i = end + 1
            continue

        # Block comment
        if char == '/' and i + 1 < length and content[i + 1] == '*':
            end = content.find('*/', i + 2)
            if end == -1:
                current.append(content[i:])
                break
            current.append(content[i : end + 2])
            i = end + 2
            continue

        # Single-quoted string
        if char == "'":
            end = _find_closing_quote(content, i, "'")
            current.append(content[i : end + 1])
            i = end + 1
            continue

        # Double-quoted identifier
        if char == '"':
            end = _find_closing_quote(content, i, '"')
            current.append(content[i : end + 1])
            i = end + 1
            continue

        # Statement delimiter
        if char == ';':
            stmt = ''.join(current)
            if stmt.strip():
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    # Remaining content after last semicolon
    remainder = ''.join(current)
    if remainder.strip():
        statements.append(remainder)

    return statements


def _find_closing_quote(content: str, start: int, quote: str) -> int:
    """Find the closing quote, handling escaped quotes."""
    i = start + 1
    length = len(content)
    while i < length:
        if content[i] == '\\':
            i += 2
            continue
        if content[i] == quote:
            # Check for doubled quote (escape in SQL)
            if i + 1 < length and content[i + 1] == quote:
                i += 2
                continue
            return i
        i += 1
    return length - 1
