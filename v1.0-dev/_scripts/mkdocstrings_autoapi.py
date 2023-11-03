# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Generate the code reference pages."""

from frequenz.repo.config.mkdocs import api_pages

api_pages.generate_python_api_pages("src", "reference")
