"""Rowset write runtime package."""

__all__ = ["run_write_rowset"]


def __getattr__(name: str):
    if name == "run_write_rowset":
        from zeta4s.runtime.write.core import run_write_rowset

        return run_write_rowset
    raise AttributeError(name)
