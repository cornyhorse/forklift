import re
from typing import List

def dedupe_column_names(names: List[str]) -> List[str]:
    """
    Ensure all names in the given list are unique by appending numeric suffixes
    (e.g., "col", "col_1", "col_2", …) when duplicates appear.

    Example:
        Input:  ["id", "name", "name", "amount", "name"]
        Output: ["id", "name", "name_1", "amount", "name_2"]

    :param names: List of original names (possibly with duplicates).
    :returns: List of deduplicated names with suffixes applied where needed.
    """
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

