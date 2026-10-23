"""install_pii_log_redaction() -- code-location-level PII/secret redaction for compute logs.

Unlike ``compute_log_managers/`` (instance-level infrastructure configured in
``dagster.yaml``, which REQUIRES control of the Dagster instance / Hybrid
agent -- impossible on Dagster+ Serverless, where the instance is fully
managed), this is a plain Python function you call once, at import time, in
your OWN code location's ``definitions.py``:

    # definitions.py
    from dagster_community_components.log_redaction import install_pii_log_redaction

    install_pii_log_redaction()  # call this BEFORE any asset/op code can run

    import dagster as dg
    defs = dg.Definitions(...)

Each Dagster code location runs as its own Python process (the gRPC user
code server). Replacing ``sys.stdout``/``sys.stderr`` once, at import time of
that process, covers every ``print()`` call and every op/asset that
subsequently runs in it -- with ZERO per-asset opt-in, and zero instance or
agent configuration. This is what makes it work identically on Dagster+
Serverless, Hybrid, and OSS.

The real trade-off (document this to your team): it is NOT automatic across
an organization's code locations the way an instance-level ComputeLogManager
is. You must add this one line, and the `presidio-analyzer`/`spacy` extra if
you enable ML-based detection, to EVERY code location whose logs might carry
PII or secrets. A code location that forgets this call has unredacted
compute logs. There is no instance-wide switch. Treat this the same way you'd
treat a required lint rule or security dependency: add a CI check in your own
pipeline that greps each code location's `definitions.py` for this call (or
ships a shared base module that every code location imports), rather than
relying on every engineer remembering it by hand.
"""
import re
import sys
from typing import IO, Any, Iterable, List, Optional

_DEFAULT_PII_ENTITIES = (
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "US_SSN",
    "CREDIT_CARD",
    "IP_ADDRESS",
)

# Regex-based PII detection. No ML dependency required -- these are the
# high-signal, low-false-positive patterns; PERSON/LOCATION-style detection
# needs real NLP (see `enable_presidio=True` below) since there is no
# reliable regex for "is this word a person's name."
_PII_PATTERNS = {
    "EMAIL_ADDRESS": [r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"],
    "PHONE_NUMBER": [
        r"\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b",
    ],
    "US_SSN": [
        r"\b\d{3}-\d{2}-\d{4}\b",
        r"\b\d{3}\s\d{2}\s\d{4}\b",
    ],
    "CREDIT_CARD": [
        r"\b(?:4\d{3}|5[1-5]\d{2}|3[47]\d{2}|6(?:011|5\d{2}))[-\s]?\d{4}[-\s]?\d{4,6}\b",
    ],
    "IP_ADDRESS": [r"\b(?:\d{1,3}\.){3}\d{1,3}\b"],
}

# Secret/credential detection -- a DIFFERENT, usually higher-value problem
# than PII: leaked API keys/tokens in stdout are a common, well-signatured
# failure mode (a misconfigured client library logging its own auth header,
# a traceback that includes a connection string). These patterns are
# intentionally specific (vendor-prefixed) to keep false positives near zero.
_SECRET_PATTERNS = {
    "AWS_ACCESS_KEY": [r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"],
    "GITHUB_TOKEN": [r"\bgh[pousr]_[A-Za-z0-9]{36,255}\b"],
    "SLACK_TOKEN": [r"\bxox[baprs]-[A-Za-z0-9-]{10,72}\b"],
    "PRIVATE_KEY": [r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"],
    "JWT": [r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"],
}

_presidio_analyzer = None  # lazy-initialized, module-level cache


def _get_presidio_analyzer():
    """Build (once) a Presidio AnalyzerEngine for ML-based entity detection
    (PERSON, LOCATION, DATE_TIME, etc. -- things no regex can reliably catch).
    Raises ImportError with a clear message if the optional deps aren't
    installed; callers should only reach this when enable_presidio=True."""
    global _presidio_analyzer
    if _presidio_analyzer is None:
        from presidio_analyzer import AnalyzerEngine
        from presidio_analyzer.nlp_engine import SpacyNlpEngine

        nlp_engine = SpacyNlpEngine(models=[{"lang_code": "en", "model_name": "en_core_web_sm"}])
        _presidio_analyzer = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])
    return _presidio_analyzer


def _redact_with_regex(text: str, patterns: dict, entities: Iterable[str], mask_format: str) -> str:
    for entity in entities:
        for pattern in patterns.get(entity, ()):
            text = re.sub(pattern, mask_format.format(entity=entity), text)
    return text


def _redact_with_presidio(text: str, entities: Iterable[str], score_threshold: float, mask_format: str) -> str:
    try:
        analyzer = _get_presidio_analyzer()
    except ImportError as e:
        raise ImportError(
            "enable_presidio=True requires the optional 'presidio-analyzer', 'presidio-anonymizer', "
            "and 'spacy' packages (plus `python -m spacy download en_core_web_sm`). "
            f"Install them, or set enable_presidio=False to use regex-only detection. Original error: {e}"
        ) from e
    results = analyzer.analyze(text=text, language="en", entities=list(entities), score_threshold=score_threshold)
    # Highest-scoring, non-overlapping spans only, right-to-left so earlier offsets stay valid.
    results = sorted(results, key=lambda r: r.start)
    kept = []
    for r in results:
        if kept and r.start < kept[-1].end:
            if r.score > kept[-1].score:
                kept[-1] = r
            continue
        kept.append(r)
    for r in sorted(kept, key=lambda r: r.start, reverse=True):
        text = text[: r.start] + mask_format.format(entity=r.entity_type) + text[r.end :]
    return text


def redact_text(
    text: str,
    *,
    pii_entities: Iterable[str] = _DEFAULT_PII_ENTITIES,
    redact_secrets: bool = True,
    enable_presidio: bool = False,
    score_threshold: float = 0.3,
    mask_format: str = "[{entity}]",
) -> str:
    """Redact PII and/or secrets from a single string. Exposed directly so you can unit-test your own
    redaction config, or call it from a place other than stdout (e.g. before writing to a different sink)."""
    if not text:
        return text
    out = text
    if redact_secrets:
        out = _redact_with_regex(out, _SECRET_PATTERNS, _SECRET_PATTERNS.keys(), mask_format)
    regex_entities = [e for e in pii_entities if e in _PII_PATTERNS]
    out = _redact_with_regex(out, _PII_PATTERNS, regex_entities, mask_format)
    if enable_presidio:
        ml_entities = [e for e in pii_entities if e not in _PII_PATTERNS]
        if ml_entities:
            out = _redact_with_presidio(out, ml_entities, score_threshold, mask_format)
    return out


class _RedactingStream:
    """A stdout/stderr replacement that redacts each write() before passing it through.

    Buffers partial lines so a PII pattern split across two write() calls
    (e.g. a library that writes a string then a trailing "\\n" separately)
    still gets caught -- redaction runs per logical line, not per write().
    """

    def __init__(self, original: IO[str], redact_fn):
        self._original = original
        self._redact_fn = redact_fn
        self._buffer = ""

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            return self._original.write(text)
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._original.write(self._redact_fn(line) + "\n")
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            self._original.write(self._redact_fn(self._buffer))
            self._buffer = ""
        self._original.flush()

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)


def install_pii_log_redaction(
    pii_entities: Optional[List[str]] = None,
    redact_secrets: bool = True,
    enable_presidio: bool = False,
    score_threshold: float = 0.3,
    mask_format: str = "[{entity}]",
) -> None:
    """Replace sys.stdout and sys.stderr, for the REST of this process's lifetime, with redacting
    wrappers. Call this once, at the top of your code location's definitions.py, before any asset/op
    code can run. Idempotent -- calling it twice is a no-op the second time.

    Args:
        pii_entities: Which PII types to redact. Regex-covered out of the box: EMAIL_ADDRESS,
            PHONE_NUMBER, US_SSN, CREDIT_CARD, IP_ADDRESS. Any other value (e.g. "PERSON", "LOCATION",
            "DATE_TIME") requires enable_presidio=True (NLP-based, no reliable regex exists). Defaults
            to the 5 regex-covered entities.
        redact_secrets: Also redact common credential/token patterns (AWS access keys, GitHub/Slack
            tokens, PEM private key headers, JWTs). Default True -- this is usually higher-value than
            PII redaction, since leaked credentials are a more common and more severe real-world
            compute-log incident than leaked personal data.
        enable_presidio: Use Microsoft Presidio + spaCy for ML-based entity detection (PERSON,
            LOCATION, DATE_TIME, and everything in Presidio's supported-entity list) in addition to the
            regex patterns. Requires the optional `presidio-analyzer`, `presidio-anonymizer`, `spacy`
            packages and `python -m spacy download en_core_web_sm`. Default False (regex-only, zero
            extra dependencies) -- turn this on only if you need name/address detection and have budget
            for the added latency and false-positive rate ML-based NER carries.
        score_threshold: Presidio confidence threshold (0.0-1.0). Ignored unless enable_presidio=True.
        mask_format: Replacement format string; `{entity}` is substituted with the matched entity type,
            e.g. the default "[{entity}]" turns an email into "[EMAIL_ADDRESS]".
    """
    entities = list(pii_entities) if pii_entities is not None else list(_DEFAULT_PII_ENTITIES)

    def _redact(line: str) -> str:
        return redact_text(
            line,
            pii_entities=entities,
            redact_secrets=redact_secrets,
            enable_presidio=enable_presidio,
            score_threshold=score_threshold,
            mask_format=mask_format,
        )

    if not isinstance(sys.stdout, _RedactingStream):
        sys.stdout = _RedactingStream(sys.stdout, _redact)
    if not isinstance(sys.stderr, _RedactingStream):
        sys.stderr = _RedactingStream(sys.stderr, _redact)


def uninstall_pii_log_redaction() -> None:
    """Restore the original sys.stdout/sys.stderr. Mainly for tests."""
    if isinstance(sys.stdout, _RedactingStream):
        sys.stdout.flush()
        sys.stdout = sys.stdout._original
    if isinstance(sys.stderr, _RedactingStream):
        sys.stderr.flush()
        sys.stderr = sys.stderr._original
