# How to wire PII/secret redaction into ONE code location. Put this at the very top of your
# definitions.py, before anything that could print() or log sensitive data runs.

from dagster_community_components.log_redaction import install_pii_log_redaction

install_pii_log_redaction(
    # Regex-covered out of the box, no extra dependencies:
    pii_entities=["EMAIL_ADDRESS", "PHONE_NUMBER", "US_SSN", "CREDIT_CARD", "IP_ADDRESS"],
    redact_secrets=True,  # AWS keys, GitHub/Slack tokens, PEM private key headers, JWTs
    enable_presidio=False,  # set True (+ pip install presidio-analyzer presidio-anonymizer spacy
                             # && python -m spacy download en_core_web_sm) for PERSON/LOCATION/DATE_TIME
)

import dagster as dg

# ... the rest of your definitions.py, unchanged ...
defs = dg.Definitions(assets=[...])
