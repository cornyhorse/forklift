import re
from typing import List

def dedupe_column_names(names: List[str], method: str = "suffix") -> List[str]:
    """
    Ensure all names in the given list are unique by appending numeric suffixes
    (e.g., "col", "col_1", "col_2", …) when duplicates appear.

    Example:
        Input:  ["id", "name", "name", "amount", "name"]
        Output: ["id", "name", "name_1", "amount", "name_2"]

    :param names: List of original names (possibly with duplicates).
    :param method: Deduplication method ("suffix", "prefix", or "error").
    :returns: List of deduplicated names with suffixes applied where needed.
    """
    if method == "error":
        seen = set()
        for name in names:
            if name in seen:
                raise ValueError(f"Duplicate column name detected: {name}")
            seen.add(name)
        return names

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
                    match = re.match(r"(.+?)(_\d+)+$", new_name)
                    if match:
                        prefix = match.group(1)
                        suffixes = re.findall(r"_\d+", new_name)
                        last_num = int(suffixes[-1][1:]) + 1
                        new_name = f"{prefix}{''.join(suffixes[:-1])}_{last_num}"

            deduped.append(new_name)
            seen_counts[base_name] = count + 1
            used_names.add(new_name)

    return deduped


def standardize_postgres_column_name(name: str) -> str:
    """
    Standardize a column name for Postgres compatibility:
    - Lowercase
    - Replace non-alphanumeric characters with underscores
    - Collapse multiple underscores
    - Strip leading/trailing underscores
    - Truncate to 63 characters (Postgres limit)

    :param name: The column name to standardize.
    :returns: Standardized column name string.
    """
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:63]
