# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Generate the code reference pages.

Based on the recipe at:
https://mkdocstrings.github.io/recipes/#automatic-code-reference-pages
"""

from pathlib import Path
from typing import Tuple

import mkdocs_gen_files

SRC_PATH = "src"
DST_PATH = "reference"


def is_internal(path_parts: Tuple[str, ...]) -> bool:
    """Tell if the path is internal judging by the parts.

    Args:
        path_parts: Path.parts of the path to check.

    Returns:
        True if the path is internal.
    """

    def with_underscore_not_init(part: str) -> bool:
        return part.startswith("_") and part != "__init__"

    return any(p for p in path_parts if with_underscore_not_init(p))


# type ignore because mkdocs_gen_files uses a very weird module-level
# __getattr__() which messes up the type system
nav = mkdocs_gen_files.Nav()  # type: ignore

for path in sorted(Path(SRC_PATH).rglob("*.py")):
    module_path = path.relative_to(SRC_PATH).with_suffix("")

    doc_path = path.relative_to(SRC_PATH).with_suffix(".md")
    full_doc_path = Path(DST_PATH, doc_path)
    parts = tuple(module_path.parts)
    if is_internal(parts):
        continue
    if parts[-1] == "__init__":
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
        parts = parts[:-1]

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as output_file:
        output_file.write(f"::: {'.'.join(parts)}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, Path("..") / path)

with mkdocs_gen_files.open(Path(DST_PATH) / "SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
