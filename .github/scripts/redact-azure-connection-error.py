"""Print bounded connection diagnostics without exposing configured Azure secrets."""

import os
import sys
from pathlib import Path


def redact(text):
    values = [os.environ.get("AZURE_SQL_" + key, "")
              for key in ("SERVER", "DATABASE", "USER", "PASSWORD")]
    for value in sorted(filter(None, values), key=len, reverse=True):
        text = text.replace(value, "[redacted]")
    # Redact before splitting/truncating, including secrets containing newlines.
    lines = text.splitlines()
    if not lines:
        lines = ["sqlcmd returned no diagnostic text."]
    # The runner trims whitespace for ::commands and searches anywhere for ##[.
    # Break both introducers before printing untrusted client text.
    return "\n".join("    " + line.replace("::", ": :").replace("##[", "# #[")[:500]
                     for line in lines[-20:])


if __name__ == "__main__":
    print(redact(Path(sys.argv[1]).read_bytes().decode(errors="replace")))
