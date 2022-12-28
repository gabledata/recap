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