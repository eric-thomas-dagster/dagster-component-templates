"""DuckDB stand-ins for the warehouse AI functions, so a test can EXECUTE the incremental statement and COUNT how often the AI is called.

  BigQuery   ML.GENERATE_EMBEDDING(MODEL m, (SELECT .., content), STRUCT(..))  -> rewritten (sqlglot AST) into a subquery calling `ai_embed`
             AI.CLASSIFY(text, [labels])                                         -> `ai_classify`
  Snowflake  SNOWFLAKE.CORTEX.EMBED_TEXT_768(model, text) / CLASSIFY_TEXT(text, labels) / COMPLETE(model, prompt) -> `ai_query` / `ai_classify` /
             `ai_generate` (text rewrite)
  Databricks ai_query(model, text) / ai_classify(text, labels)                    -> registered under their own names

Each stand-in is a Python UDF that records its call (model + text / text + labels), so `calls.embed` / `calls.classify` are exactly the rows the
warehouse would have billed for. The vectors are deterministic functions of (model, text), so a stale reuse (wrong model) is visible in the values.
These are NOT the real functions: return types (BigQuery ARRAY<FLOAT64>, Snowflake VECTOR, Databricks ARRAY<FLOAT>, Cortex's CLASSIFY_TEXT OBJECT)
and failure modes are not modelled.

enrich (ML.GENERATE_TEXT / CORTEX.COMPLETE / ai_query): Databricks' real `ai_query` is genuinely the same function for both embeddings and
generation (whatever its target model endpoint returns), so this component's generated SQL calls it under that one name for both -- but a single
DuckDB UDF can only declare one return type, so this stub tells the two calls apart by argument SHAPE instead: enrich's SQL always wraps its
prompt in CONCAT(...), embed's never does (see `rewrite_text` below). BigQuery's ML.GENERATE_TEXT/AI.GENERATE_TEXT has no sqlglot support for its
MODEL/TABLE/STRUCT table-function syntax as of sqlglot 28.0.0 (confirmed directly: sqlglot.parse_one raises ParseError on both spellings) -- the
BigQuery enrich path is therefore verified structurally (string assertions on the generated SQL, checked against the real docs) rather than
executed, unlike everything else in execution_mode='sql'.
"""
import re
import zlib
from typing import List, Tuple

import duckdb
import sqlglot
from sqlglot import exp


class AiCalls:
    def __init__(self):
        self.embed: List[Tuple[str, str]] = []              # (model, text)
        self.classify: List[Tuple[str, Tuple[str, ...]]] = []   # (text, labels)
        self.enrich: List[Tuple[str, str]] = []              # (model, prompt)

    def reset(self):
        self.embed.clear()
        self.classify.clear()
        self.enrich.clear()


def vector_for(model: str, text: str) -> List[float]:
    return [float(len(text)), float(zlib.crc32(text.encode()) % 997), float(zlib.crc32(model.encode()) % 991)]


def label_for(text: str, labels) -> str:
    return list(labels)[zlib.crc32(text.encode()) % len(labels)]


def generated_for(model: str, prompt: str) -> str:
    return f"GEN[{model}]:{zlib.crc32(prompt.encode()) % 997}"


def register(con: "duckdb.DuckDBPyConnection") -> AiCalls:
    calls = AiCalls()

    def ai_embed(model, text):
        calls.embed.append((model, text))
        return vector_for(model, text)

    def ai_classify(text, labels):
        calls.classify.append((text, tuple(labels)))
        return label_for(text, labels)

    def ai_generate(model, prompt):
        calls.enrich.append((model, prompt))
        return generated_for(model, prompt)

    con.create_function("ai_embed", ai_embed, ["VARCHAR", "VARCHAR"], "DOUBLE[]")
    con.create_function("ai_query", ai_embed, ["VARCHAR", "VARCHAR"], "DOUBLE[]")    # Databricks' embed name
    con.create_function("ai_generate", ai_generate, ["VARCHAR", "VARCHAR"], "VARCHAR")    # Snowflake's name + Databricks' enrich name (disambiguated below)
    con.create_function("ai_classify", ai_classify, ["VARCHAR", "VARCHAR[]"], "VARCHAR")
    # Snowflake CLASSIFY_TEXT returns an OBJECT {"label": ...} (not the label): modelled as a JSON object, so SQL that stores it without
    # extracting :label stores '{"label": ..}' instead of the bare label.
    con.execute("CREATE OR REPLACE MACRO cortex_classify_text(t, l) AS CAST(json_object('label', ai_classify(t, l)) AS JSON)")
    return calls


def rewrite_bigquery(stmt: exp.Expression) -> exp.Expression:
    """FROM ML.GENERATE_EMBEDDING(MODEL m, (subquery with a `content` column), STRUCT(..)) -> (SELECT i.*, ai_embed('m', i.content) AS
    ml_generate_embedding_result FROM (subquery) AS i), which is what the table function returns (the input columns + the result column)."""
    for ge in list(stmt.find_all(exp.GenerateEmbedding)):
        table = ge.parent
        model = ".".join(p.name for p in (ge.this.args.get("catalog"), ge.this.args.get("db"), ge.this) if p is not None)
        inner = ge.expression.copy().as_("i")
        select = exp.select("i.*", exp.alias_(exp.Anonymous(this="ai_embed", expressions=[exp.Literal.string(model), exp.column("content", "i")]),
                                              "ml_generate_embedding_result")).from_(inner)
        table.replace(exp.Subquery(this=select, alias=exp.TableAlias(this=exp.to_identifier("ge"))))
    return stmt


def rewrite_text(sql: str) -> str:
    # Databricks' ai_query(...) serves both embed and enrich under the one real name; this stub tells them
    # apart by argument shape -- enrich's second argument is always CONCAT(...) (the prompt), embed's never is.
    sql = re.sub(r"\bai_query\(([^,]+),\s*CONCAT\(", r"ai_generate(\1, CONCAT(", sql)
    return (sql.replace("SNOWFLAKE.CORTEX.EMBED_TEXT_768(", "ai_query(")
               .replace("SNOWFLAKE.CORTEX.CLASSIFY_TEXT(", "cortex_classify_text(")
               .replace("SNOWFLAKE.CORTEX.COMPLETE(", "ai_generate(")
               .replace("AI.CLASSIFY(", "ai_classify("))
