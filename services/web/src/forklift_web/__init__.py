"""forklift-web: the gateway of the forklift platform (docs/design/platform.md, section 5).

The gateway logs people in, keeps the schema registry, datasets, connections and the job queue
in PostgreSQL, and signs URLs so that browsers, clients and workers move data to and from the
object store directly. It never imports the engine (``forklift``) or ``pyarrow`` and never reads
object contents.
"""

__version__ = "0.1.0"
