# Developer Notes

## Tests

Install the `tests` dev-dependencies:

    pdm install -dG tests

Run the unit and spec tests together:

    pdm run test

### Unit Tests

Run the unit tests:

    pdm run unit

### Spec Tests

Recap has spec validation tests that validate Recap various Recap schemas against [Recap's type spec](https://recap.build/specs/type/). These tests are located in the `tests/spec` directory.

Run the spec tests:

    pdm run spec

These tests require internet access because they download the spec from Recap's website.

### Integration Tests

Recap has integration tests that validate some of Recap's readers against real systems. These tests are located in the `tests/integration` directory.

Run the integration tests:

    pdm run integration

These test require various systems to be running as defined in the .github/workflows/ci.yml file.

## Style

Install the `style` dev-dependencies:

    pdm install -dG style

Recap does style checks, formatting, linting, and type hint checks with:

* [`black`](https://github.com/psf/black)
* [`isort`](https://github.com/PyCQA/isort)
* [`autoflake`](https://github.com/PyCQA/autoflake)
* [`pylint`](https://github.com/PyCQA/pylint)
* [`pyright`](https://github.com/microsoft/pyright)

### Formatting

Recap uses for code formatting and  for import sorting.

Format the code with:

    pdm run style

Run individually with:

    pdm run black recap/ tests/
    pdm run isort recap/ tests/
    pdm run autoflake --in-place --remove-unused-variables --remove-all-unused-imports --recursive recap/ tests/

If just want to see what will be formatted without changing anything:

    pdm run black --check --diff recap/ tests/
    pdm run isort --check --diff recap/ tests/
    pdm run autoflake --check-diff --remove-unused-variables --remove-all-unused-imports --recursive recap/ tests/

### Linting

Run linting:

    pdm run pylint --fail-under=7.0 recap/ tests/

### Type Checks

Run type checks:

    pdm run pyright
