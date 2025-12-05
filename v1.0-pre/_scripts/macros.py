# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""This module defines macros for use in Markdown files."""

import os
import pathlib

from frequenz.repo.config.mkdocs.mkdocstrings_macros import (
    hook_env_with_everything,
    slugify,
)
from mkdocs_macros import plugin as macros


def define_env(env: macros.MacrosPlugin) -> None:
    """Define the hook to create macro functions for use in Markdown.

    Args:
        env: The environment to define the macro functions in.
    """

    @env.macro  # type: ignore[untyped-decorator]
    def glossary(term: str, text: str | None = None) -> str:
        """Create a link to the glossary entry for the given term.

        Args:
            term: The term to link to.
            text: The text to display for the link. Defaults to the term.

        Returns:
            The Markdown link to the glossary entry for the given term.
        """
        current_path = pathlib.Path(env.page.file.src_uri)
        glossary_path = pathlib.Path("user-guide/glossary.md")
        # This needs to use `os.path.relpath` instead of `pathlib.Path.relative_to`
        # because the latter expects one path to be a parent of the other, which is not
        # always the case, for example when referencing the glossary from the API
        # reference.
        link_path = os.path.relpath(glossary_path, current_path.parent)
        return f"[{text or term}]({link_path}#{slugify(term)})"

    # This must be at the end to enable all standard features
    hook_env_with_everything(env)
