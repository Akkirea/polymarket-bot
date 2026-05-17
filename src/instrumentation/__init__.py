"""Pure observability modules. Removable without behavior impact.

Each submodule under this package is additive: deleting the package and the
two-or-three import/call sites that reference it must leave the bot's runtime
behavior unchanged.
"""
