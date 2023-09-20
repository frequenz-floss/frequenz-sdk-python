# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""This module defines macros for use in Markdown files."""

import pathlib

from markdown.extensions import toc
from mkdocs_macros import plugin as macros

_CODE_ANNOTATION_MARKER: str = (
    r'<span class="md-annotation">'
    r'<span class="md-annotation__index" tabindex="-1">'
    r'<span data-md-annotation-id="1"></span>'
    r"</span>"
    r"</span>"
)


def _slugify(text: str) -> str:
    """Slugify a text.

    Args:
        text: The text to slugify.

    Returns:
        The slugified text.
    """
    # The type of the return value is not defined for the markdown library.
    # Also for some reason `mypy` thinks the `toc` module doesn't have a
    # `slugify_unicode` function, but it definitely does.
    return toc.slugify_unicode(text, "-")  # type: ignore[attr-defined,no-any-return]


def define_env(env: macros.MacrosPlugin) -> None:
    """Define the hook to create macro functions for use in Markdown.

    Args:
        env: The environment to define the macro functions in.
    """
    # A variable to easily show an example code annotation from mkdocs-material.
    # https://squidfunk.github.io/mkdocs-material/reference/code-blocks/#adding-annotations
    env.variables["code_annotation_marker"] = _CODE_ANNOTATION_MARKER

    @env.macro  # type: ignore[misc]
    def glossary(term: str) -> str:
        """Create a link to the glossary entry for the given term.

        Args:
            term: The term to link to.

        Returns:
            The Markdown link to the glossary entry for the given term.
        """
        current_path = pathlib.Path(env.page.file.src_uri)
        glossary_path = pathlib.Path("intro/glossary.md")
        link_path = glossary_path.relative_to(current_path.parent)
        return f"[{term}]({link_path}#{_slugify(term)})"
