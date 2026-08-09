"""Deterministic key generation and text normalisation.

Every surrogate key, hash-diff and match key in the platform is produced here.
The one rule these must all obey: **the same input always yields the same key, on
every run, on every machine**. That is what makes the pipeline idempotent — a
re-run recomputes identical keys and the MERGEs become no-ops. Anything derived
from ``monotonically_increasing_id()``, row order, or partition count would break
that guarantee, so none of it appears in this module.
"""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

# Honorifics and titles that appear in Indian hospital registration data. Stripping
# these is what lets "Dr. Rajesh Kumar" and "Rajesh Kumar" collapse to one person.
TITLES = (
    "MR",
    "MRS",
    "MS",
    "MISS",
    "DR",
    "PROF",
    "SHRI",
    "SMT",
    "SRI",
    "KUM",
    "MASTER",
    "BABY",
    "B/O",  # "baby of", common on neonatal registrations
)

_TITLE_PATTERN = r"^(?:" + "|".join(TITLES) + r")\.?\s+"

NULL_SENTINEL = "__NULL__"


def normalize_name(col: Column) -> Column:
    """Uppercase, strip titles and punctuation, collapse whitespace.

    Applied repeatedly (titles can stack: "DR. MRS. PRIYA") until stable — two
    passes covers every case observed in the source data.
    """
    cleaned = F.upper(F.trim(col))
    cleaned = F.regexp_replace(cleaned, r"[^A-Z\s]", " ")  # drop digits/punctuation
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)
    for _ in range(2):
        cleaned = F.trim(F.regexp_replace(cleaned, _TITLE_PATTERN, ""))
    return F.regexp_replace(cleaned, r"\s+", " ")


def normalize_phone(col: Column) -> Column:
    """Reduce a phone number to its last 10 digits.

    Source systems record the same number as ``+91-98765 43210``, ``09876543210``
    and ``9876543210``. Keeping the last 10 digits normalises country code and
    trunk prefix away without losing the subscriber number.
    """
    digits = F.regexp_replace(F.coalesce(col, F.lit("")), r"\D", "")
    return F.when(F.length(digits) >= 10, F.substring(digits, -10, 10)).otherwise(F.lit(None))


def phone_last4(col: Column) -> Column:
    """Last four digits of a normalised phone number, used in the MPI match key."""
    normalized = normalize_phone(col)
    return F.when(normalized.isNotNull(), F.substring(normalized, -4, 4)).otherwise(F.lit(None))


def sha256_key(*cols: Column) -> Column:
    """SHA-256 over pipe-joined parts, with nulls made explicit.

    ``concat_ws`` skips nulls, so ``("A", null, "B")`` and ``("A", "B", null)``
    would hash identically. Substituting a sentinel keeps distinct inputs distinct.
    """
    parts = [F.coalesce(c.cast("string"), F.lit(NULL_SENTINEL)) for c in cols]
    return F.sha2(F.concat_ws("|", *parts), 256)


def hash_diff(*cols: Column) -> Column:
    """Change-detection hash over an SCD2 dimension's tracked attributes.

    Comparing one hash is both cheaper and less error-prone than comparing a dozen
    nullable columns pairwise (``a <> b`` is null, not true, when either side is
    null — a classic source of missed SCD2 versions).
    """
    return sha256_key(*cols)


def surrogate_key(*cols: Column) -> Column:
    """Stable 63-bit surrogate key derived from business key columns.

    Hash-based rather than sequential: it needs no coordination, survives
    re-runs and parallel writes, and lets facts compute their dimension keys
    without reading the dimension table. The top bit is masked off to keep the
    value positive in a signed BIGINT.
    """
    return F.abs(
        F.xxhash64(
            F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit(NULL_SENTINEL)) for c in cols])
        )
    )


def clean_python_name(value: str | None) -> str:
    """Pure-Python mirror of :func:`normalize_name`, for tests and the generator.

    Kept deliberately in lockstep with the Spark version; ``tests/unit`` asserts the
    two agree on a shared corpus of awkward names.
    """
    if value is None:
        return ""
    text = re.sub(r"[^A-Z\s]", " ", value.upper())
    text = re.sub(r"\s+", " ", text).strip()
    for _ in range(2):
        text = re.sub(_TITLE_PATTERN, "", text).strip()
    return re.sub(r"\s+", " ", text)


def clean_python_phone(value: str | None) -> str | None:
    """Pure-Python mirror of :func:`normalize_phone`."""
    if value is None:
        return None
    digits = re.sub(r"\D", "", value)
    return digits[-10:] if len(digits) >= 10 else None
