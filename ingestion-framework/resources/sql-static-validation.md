
The best fit today is sqlglot. It has a real SQL grammar, supports an ANSI dialect, and throws precise parse errors (line/column) without running anything. Contrast:
sqlglot ✅ parses & validates syntax (ANSI + many dialects), gives AST, raises on errors.
sqlparse ❌ formatter/tokenizer only—no syntax validation.
moz-sql-parser ⚠️ partial coverage, dated.
sqlfluff ✅ lints and parses, but aims at style as much as syntax and its dialects don’t perfectly match Flink; still useful, but heavier.

Below is exactly how I’d add static validation into your v3 generator, staying ANSI-first and avoiding any runtime/cluster dependency.

What we’ll validate
Generated statements in the single SQL file:
CREATE VIEW ... AS SELECT ...; → validate as is (ANSI).
CREATE TABLE ... WITH (...); → validate after stripping the Flink WITH (...) property block (ANSI doesn’t know it).
INSERT INTO ... SELECT ...; → validate as is (ANSI).
EXECUTE STATEMENT SET … → unwrap and validate each inner INSERT separately.

Per-cell Expressions in STTM:
For each non-empty Expression, wrap into a dummy select like
SELECT <expr> AS _x FROM t; and parse (names don’t need to exist for syntax validation).
This approach catches typos like missing commas, bad CASE syntax, unbalanced parentheses, malformed regex strings, etc., before you ever ship SQL.