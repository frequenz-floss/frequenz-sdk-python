# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Generate the code reference pages."""

from frequenz.repo.config import mkdocs

mkdocs.generate_python_api_pages("src", "reference")
