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

## Code Formatting

Install the `style` dev-dependencies:

    pdm install -dG style

Run the format using [`black`](https://github.com/psf/black) and [`isort`](https://github.com/PyCQA/isort):

    pdm run black recap/ tests/
    pdm run isort recap/ tests/

If you want only to check the diff that will be formatted:

    pdm run black --check --diff recap/ tests/
    pdm run isort --check --diff recap/ tests/

## Code Linting

Install the `style` dev-dependencies:

    pdm install -dG style

Run the format using [`pylint`](https://github.com/PyCQA/pylint):

    pdm run pylint --fail-under=7.0 recap/ tests/

For static type checker, we use [`pyright`](https://github.com/microsoft/pyright), you can [install](https://github.com/microsoft/pyright#installation) on your favorite IDE, or simply run the following command:

    pdm run pyright
