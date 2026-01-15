#!/usr/bin/env python3
"""Clear locally persisted settings and tokens.

This removes:
- data/local_connection_settings.json (non-sensitive settings)
- Tokens stored in the OS keyring (Windows Credential Manager on Windows)

Useful before publishing the repo or when you want a completely clean state.
"""

from __future__ import annotations

from storage import clear_connection_settings


def main() -> None:
    clear_connection_settings()
    print("Cleared local connection settings (file + OS keyring tokens).")


if __name__ == "__main__":
    main()
