"""Elasticsearch runtime backend."""

__all__ = ["open_elasticsearch_rowset_reader", "run_elasticsearch_command", "write_elasticsearch_rowset"]


def __getattr__(name):
    if name == "open_elasticsearch_rowset_reader":
        from zeta4s.runtime.backends.elasticsearch.extract import open_elasticsearch_rowset_reader

        return open_elasticsearch_rowset_reader
    if name == "run_elasticsearch_command":
        from zeta4s.runtime.backends.elasticsearch.command import run_elasticsearch_command

        return run_elasticsearch_command
    if name == "write_elasticsearch_rowset":
        from zeta4s.runtime.backends.elasticsearch.write import write_elasticsearch_rowset

        return write_elasticsearch_rowset
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
