import hashlib
import re
import unicodedata
from typing import List, Optional

# PostgreSQL identifiers are limited to 63 bytes (all characters are ASCII after standardizing)
POSTGRES_MAX_IDENTIFIER_LENGTH = 63


def dedupe_column_names(
    names: List[str], method: str = "suffix", max_length: Optional[int] = None
) -> List[str]:
    """
    Ensure all names in the given list are unique by appending numeric suffixes
    (e.g., "col", "col_1", "col_2", …) when duplicates appear.

    Always terminates: the result has the same length as the input and no duplicates, also for
    empty names (``["", "", ""]`` -> ``["", "_1", "_2"]``) and names that already look like
    generated ones (``["a", "a", "a_1"]``).

    Example:
        Input:  ["id", "name", "name", "amount", "name"]
        Output: ["id", "name", "name_1", "amount", "name_2"]

    :param names: List of original names (possibly with duplicates).
    :param method: Deduplication method ("suffix", "prefix", or "error").
    :param max_length: Optional maximum name length (e.g. 63 for Postgres). When set, names are
        cut to that length and the base of a generated name is shortened so that
        ``base + suffix`` still fits. Without it, suffixes simply extend the name.
    :returns: List of deduplicated names with suffixes applied where needed.
    """
    if method == "error":
        seen = set()
        for name in names:
            if name in seen:
                raise ValueError(f"Duplicate column name detected: {name}")
            seen.add(name)
        return names

    if max_length is not None:
        if max_length < 16:
            raise ValueError("max_length must be at least 16 to leave room for suffixes")
        return _dedupe_bounded(names, method, max_length)

    seen_counts: dict[str, int] = {}
    deduped: list[str] = []
    used_names: set[str] = set()

    for name in names:
        base_name = name
        count = seen_counts.get(base_name, 0)

        if count == 0 and base_name not in used_names:
            deduped.append(base_name)
            seen_counts[base_name] = 1
            used_names.add(base_name)
        else:
            if method == "prefix":
                new_name = f"1_{base_name}"  # Start at 1_ for first duplicate
                counter = 1
                while new_name in used_names:
                    counter += 1
                    new_name = f"{counter}_{base_name}"
            else:  # default to "suffix"
                new_name = f"{base_name}_1"  # Start at _1 for first duplicate
                while new_name in used_names:
                    # Find the last numeric suffix and increment it
                    suffixes = re.findall(r"_\d+", new_name)
                    last_num = int(suffixes[-1][1:]) + 1
                    match = re.match(r"(.+?)(_\d+)+$", new_name)
                    if match:
                        prefix = match.group(1)
                        new_name = f"{prefix}{''.join(suffixes[:-1])}_{last_num}"
                    else:
                        # The whole name is numeric suffixes (e.g. "_1", generated for an empty
                        # base name). There is no prefix for the regex to anchor on; this used to
                        # leave new_name unchanged and loop forever.
                        new_name = f"{''.join(suffixes[:-1])}_{last_num}"

            deduped.append(new_name)
            seen_counts[base_name] = count + 1
            used_names.add(new_name)

    return deduped


def _dedupe_bounded(names: List[str], method: str, max_length: int) -> List[str]:
    """Deduplicate with a length limit: shorten the base so base + suffix/prefix fits."""
    deduped: list[str] = []
    used_names: set[str] = set()

    for name in names:
        candidate = name[:max_length]
        counter = 0
        while candidate in used_names:
            counter += 1
            if method == "prefix":
                affix = f"{counter}_"
                candidate = affix + name[: max_length - len(affix)]
            else:
                affix = f"_{counter}"
                candidate = name[: max_length - len(affix)] + affix
        deduped.append(candidate)
        used_names.add(candidate)

    return deduped


# Latin letters that do not decompose under NFKD but have an obvious ASCII spelling
_TRANSLITERATIONS = {
    "ß": "ss",
    "æ": "ae",
    "œ": "oe",
    "ø": "o",
    "đ": "d",
    "ð": "d",
    "ł": "l",
    "þ": "th",
    "ı": "i",
    "ħ": "h",
}


def _transliterate(name: str) -> str:
    """Best-effort ASCII form of ``name``: ``Café`` -> ``Cafe``, ``ß`` -> ``ss``."""
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return "".join(_TRANSLITERATIONS.get(ch.lower(), ch) for ch in stripped)


def standardize_postgres_column_name(name: str, index: Optional[int] = None) -> str:
    """
    Standardize a column name for Postgres compatibility:
    - Transliterate accents (``Café`` -> ``cafe``)
    - Lowercase
    - Replace non-alphanumeric characters with underscores
    - Collapse multiple underscores
    - Strip leading/trailing underscores
    - Truncate to 63 characters (Postgres limit)

    A header whose letters/digits are all non-Latin (``名前``) cannot be transliterated; instead of
    collapsing to the empty string it becomes ``col_<index>`` (when ``index`` is given) or
    ``col_<8 hex chars of a hash of the name>`` so the result is stable and still unique-ish.
    Headers without any letters or digits (``""``, ``"@#$"``) still become ``""``.

    Deduplicating afterwards appends suffixes; pass ``max_length=POSTGRES_MAX_IDENTIFIER_LENGTH``
    to ``dedupe_column_names`` to keep the final names within the limit.

    :param name: The column name to standardize.
    :param index: Optional column position used for the ``col_N`` fallback name.
    :returns: Standardized column name string.
    """
    s = _transliterate(name.strip()).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")

    if not s and any(ch.isalnum() for ch in name):
        if index is not None:
            return f"col_{index}"
        digest = hashlib.sha256(
            unicodedata.normalize("NFC", name.strip()).encode("utf-8")
        ).hexdigest()
        return f"col_{digest[:8]}"

    return s[:POSTGRES_MAX_IDENTIFIER_LENGTH]
