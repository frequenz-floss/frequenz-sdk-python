# Contributing to `frequenz-sdk`

## Build

You can use `build` to simply build the source and binary distribution:

```sh
python -m pip install build
python -m build
```

## Local development

You can use editable installs to develop the project locally (it will install
all the dependencies too):

```sh
python -m pip install -e .
```

You can also use `nox` to run the tests and other checks:

```sh
python -m pip install nox
nox
```

You can also use `nox -R` to reuse the current testing environment to speed up
test at the expense of a higher chance to end up with a dirty test environment.

### Running tests individually

For a better development test cycle you can install the runtime and test
dependencies and run `pytest` manually.

```sh
python -m pip install .
python -m pip install pytest pytest-asyncio

# And for example
pytest tests/test_sdk.py
```

To build the documentation, first install the dependencies:

```sh
python -m pip install -e .[docs]
```

Then you can build the documentation (it will be written in the `site/`
directory):

```sh
mkdocs build
```

Or you can just serve the documentation without building it using:

```sh
mkdocs serve
```

Your site will be updated **live** when you change your files (provided that
you used `pip install -e .`, beware of a common pitfall of using `pip install`
without `-e`, in that case the API reference won't change unless you do a new
`pip install`).

To build multi-version documentation, we use
[mike](https://github.com/jimporter/mike). If you want to see how the
multi-version sites looks like locally, you can use:

```sh
mike deploy my-version
mike set-default my-version
mike serve
```

`mike` works in mysterious ways. Some basic information:

* `mike deploy` will do a `mike build` and write the results to your **local**
  `gh-pages` branch. `my-version` is an arbitrary name for the local version
  you want to preview.
* `mike set-default` is needed so when you serve the documentation, it goes to
  your newly produced documentation by default.
* `mike serve` will serve the contents of your **local** `gh-pages` branch. Be
  aware that, unlike `mkdocs serve`, changes to the sources won't be shown
  live, as the `mike deploy` step is needed to refresh them.

Be careful not to use `--push` with `mike deploy`, otherwise it will push your
local `gh-pages` branch to the `origin` remote.

That said, if you want to test the actual website in **your fork**, you can
always use `mike deploy --push --remote your-fork-remote`, and then access the
GitHub pages produced for your fork.

## Releasing

These are the steps to create a new release:

1. Get the latest head you want to create a release from.

2. Update the `RELEASE_NOTES.md` file if it is not complete, up to date, and
   clean from template comments (`<!-- ... ->`) and empty sections. Submit
   a pull request if an update is needed, wait until it is merged, and update
   the latest head you want to create a release from to get the new merged pull
   request.

3. Create a new signed tag using the release notes and
   a [semver](https://semver.org/) compatible version number with a `v` prefix,
   for example:

   ```sh
   git tag -s -F RELEASE_NOTES.md v0.0.1
   ```

4. Push the new tag.

5. A GitHub action will test the tag and if all goes well it will create
   a [GitHub
   Release](https://github.com/frequenz-floss/frequenz-sdk-python/releases),
   create a new
   [announcement](https://github.com/frequenz-floss/frequenz-sdk-python/discussions/categories/announcements)
   about the release, and upload a new package to
   [PyPI](https://pypi.org/project/frequenz-sdk/) automatically.

6. Once this is done, reset the `RELEASE_NOTES.md` with the template:

   ```sh
   cp .github/RELEASE_NOTES.template.md RELEASE_NOTES.md
   ```

   Commit the new release notes and create a PR (this step should be automated
   eventually too).

7. Celebrate!
