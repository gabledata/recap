# Developer Notes

## Documentation

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

Recap uses [`pyright`](https://github.com/microsoft/pyright) for static type checking. You can use [install](https://github.com/microsoft/pyright#installation) on your favorite IDE or run the following command:

    pdm run pyright
