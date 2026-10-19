"""DuckDB stand-ins for the warehouse AI functions, so a test can EXECUTE the incremental statement and COUNT how often the AI is called.

  BigQuery   ML.GENERATE_EMBEDDING(MODEL m, (SELECT .., content), STRUCT(..))  -> rewritten (sqlglot AST) into a subquery calling `ai_embed`
             AI.CLASSIFY(text, [labels])                                         -> `ai_classify`
  Snowflake  SNOWFLAKE.CORTEX.EMBED_TEXT_768(model, text) / CLASSIFY_TEXT(text, labels) -> `ai_query` / `ai_classify` (text rewrite)
  Databricks ai_query(model, text) / ai_classify(text, labels)                    -> registered under their own names

Each stand-in is a Python UDF that records its call (model + text / text + labels), so `calls.embed` / `calls.classify` are exactly the rows the
warehouse would have billed for. The vectors are deterministic functions of (model, text), so a stale reuse (wrong model) is visible in the values.
These are NOT the real functions: return types (BigQuery ARRAY<FLOAT64>, Snowflake VECTOR, Databricks ARRAY<FLOAT>, Cortex's CLASSIFY_TEXT OBJECT)
and failure modes are not modelled.
"""
import zlib
from typing import List, Tuple

import duckdb
import sqlglot
from sqlglot import exp


class AiCalls:
    def __init__(self):
        self.embed: List[Tuple[str, str]] = []              # (model, text)
        self.classify: List[Tuple[str, Tuple[str, ...]]] = []   # (text, labels)

    def reset(self):
        self.embed.clear()
        self.classify.clear()


def vector_for(model: str, text: str) -> List[float]:
    return [float(len(text)), float(zlib.crc32(text.encode()) % 997), float(zlib.crc32(model.encode()) % 991)]


def label_for(text: str, labels) -> str:
    return list(labels)[zlib.crc32(text.encode()) % len(labels)]


def register(con: "duckdb.DuckDBPyConnection") -> AiCalls:
    calls = AiCalls()

    def ai_embed(model, text):
        calls.embed.append((model, text))
        return vector_for(model, text)

    def ai_classify(text, labels):
        calls.classify.append((text, tuple(labels)))
        return label_for(text, labels)

    con.create_function("ai_embed", ai_embed, ["VARCHAR", "VARCHAR"], "DOUBLE[]")
    con.create_function("ai_query", ai_embed, ["VARCHAR", "VARCHAR"], "DOUBLE[]")    # Databricks' name
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
    return (sql.replace("SNOWFLAKE.CORTEX.EMBED_TEXT_768(", "ai_query(")
               .replace("SNOWFLAKE.CORTEX.CLASSIFY_TEXT(", "cortex_classify_text(")
               .replace("AI.CLASSIFY(", "ai_classify("))
