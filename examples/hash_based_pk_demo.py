"""Alternative hash-based composite primary key handling for large datasets."""

import hashlib
from typing import Set, List, Any


class HashBasedPrimaryKeyTracker:
    """Alternative implementation using hashing for composite primary keys.

    This approach trades some debugging capability for better memory efficiency
    and performance with large datasets containing long composite keys.
    """

    def __init__(self):
        self.seen_hashes: Set[str] = set()
        self.hash_to_values: dict = {}  # Optional: keep mapping for debugging

    def create_composite_key_hash(self, values: List[Any]) -> str:
        """Create a hash from composite primary key values.

        Args:
            values: List of values from primary key columns

        Returns:
            SHA-256 hash string of the composite key
        """
        # Convert all values to strings and concatenate with separator
        # Use a separator that won't appear in data to avoid collisions
        key_string = "||".join(str(v) if v is not None else "NULL" for v in values)

        # Create SHA-256 hash
        return hashlib.sha256(key_string.encode("utf-8")).hexdigest()

    def is_duplicate(self, values: List[Any], store_original: bool = False) -> bool:
        """Check if composite key is duplicate and optionally store original values.

        Args:
            values: Primary key column values
            store_original: Whether to store original values for debugging

        Returns:
            True if duplicate, False if unique
        """
        key_hash = self.create_composite_key_hash(values)

        if key_hash in self.seen_hashes:
            return True
        else:
            self.seen_hashes.add(key_hash)
            if store_original:
                self.hash_to_values[key_hash] = tuple(values)
            return False

    def get_original_values(self, values: List[Any]) -> tuple:
        """Get original values for a given composite key (if stored).

        Args:
            values: Primary key column values

        Returns:
            Original tuple of values, or None if not stored
        """
        key_hash = self.create_composite_key_hash(values)
        return self.hash_to_values.get(key_hash)


# Example usage comparison
def demonstrate_approaches():
    """Show memory and performance differences between tuple and hash approaches."""
    import sys

    # Tuple approach (current implementation)
    tuple_tracker = set()

    # Hash approach
    hash_tracker = HashBasedPrimaryKeyTracker()

    # Sample composite keys with long strings (realistic scenario)
    sample_keys = [
        (
            12345,
            "john.smith@verylongcompanyname.com",
            "2024-01-01T10:30:00Z",
            "Department of Advanced Engineering Solutions",
        ),
        (
            12346,
            "jane.doe@anotherlongcompanyname.org",
            "2024-01-01T10:31:00Z",
            "Department of Advanced Engineering Solutions",
        ),
        (
            12347,
            "bob.wilson@yetanotherlongcompany.net",
            "2024-01-01T10:32:00Z",
            "Department of Advanced Engineering Solutions",
        ),
    ]

    # Add to tuple tracker
    for key in sample_keys:
        tuple_tracker.add(key)

    # Add to hash tracker
    for key in sample_keys:
        hash_tracker.is_duplicate(list(key), store_original=True)

    # Compare memory usage (rough estimate)
    tuple_size = sys.getsizeof(tuple_tracker)
    for item in tuple_tracker:
        tuple_size += sys.getsizeof(item)
        for value in item:
            tuple_size += sys.getsizeof(value)

    hash_size = sys.getsizeof(hash_tracker.seen_hashes)
    for hash_val in hash_tracker.seen_hashes:
        hash_size += sys.getsizeof(hash_val)

    print(f"Tuple approach memory: ~{tuple_size} bytes")
    print(f"Hash approach memory: ~{hash_size} bytes")
    print(f"Memory ratio: {tuple_size / hash_size:.2f}x")

    # Test collision resistance
    collision_test_keys = [
        (1, "test", "2024-01-01"),
        (1, "test||2024", "01-01"),  # Potential collision if separator not chosen carefully
    ]

    print("\nCollision test:")
    for key in collision_test_keys:
        hash_val = hash_tracker.create_composite_key_hash(list(key))
        print(f"Key {key} -> Hash: {hash_val[:16]}...")


if __name__ == "__main__":
    demonstrate_approaches()
