# Developer Notes

## Documentation

### Serving Documentation Locally

Install the `docs` dev-dependencies:

    pdm install -dG docs

Then serve using `mkdocs serve`:

    pdm run mkdocs serve

You notice that mkdocs doesn't display other versions with `mike` in local mode. You must use `mike serve` to see versions locally:

    pdm run mike serve

I don't often use `mike serve` because it doesn't auto-refresh the browser when I modify documentation locally.

## Tests

Install the `tests` dev-dependencies:

    pdm install -dG tests

Run the tests using `pytest`:

    pdm run pytest

## Code formatting

Install the `fmt` dev-dependencies:

    pdm install -dG fmt

Run the format using `black` and `isort`:

    pdm run black recap/ tests/
    pdm run isort recap/ tests/

If you want only to check the diff that will be formatted:

    pdm run black --check --diff recap/ tests/
    pdm run isort --check --diff recap/ tests/

## Code linting

Install the `lint` dev-dependencies:

    pdm install -dG lint

Run the format using `pylint`:

    pdm run pylint --fail-under=7.0 recap/ tests/
