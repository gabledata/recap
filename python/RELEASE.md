# Releasing Recap

## Configuring PDM

You must set your username and password in PDM before you can publish to PyPI.

```
pdm config repository.pypi.username "<prod pypi username>"
pdm config repository.pypi.password "<prod pypi password>"
pdm config repository.testpypi.username "<test pypi username>"
pdm config repository.testpypi.password "<test pypi password>"
```

## Publishing a Release

Publication takes three steps:

1. Tag the release on Github
2. Publish to `testpypi`
3. Publish to `pypi`

### Tag the Release on Github

1. Go to Github's [Draft new release](https://github.com/recap-cloud/recap/releases/new) page.
2. Click "Choose tag" and create a new tag with Recap's next version number.
3. Use the version number as the title.
4. Click "Generate release notes".
5. Fill in a description for the release above the release notes.
6. Click "Publish release".

### Publish PyPI

Start by publishing Recap to PyPI's test repo:

    pdm publish -r testpypi

Validate the release on [https://test.pypi.org/project/recap-core/](https://test.pypi.org/project/recap-core/), then publish to production:

    pdm publish

Bump the version in `pyproject.toml` and merge into `main`.

## Documentation

Recap uses [MkDocs](https://www.mkdocs.org/), [Material](https://squidfunk.github.io/mkdocs-material/), and [mike](https://github.com/jimporter/mike) for documentation. MkDocs is the documentation framework, Material is the theme, and mike a version manager for MkDocs.

### Releasing Documentation

Make sure you're up to date before releasing docs:

Always release from main:

    git checkout main

Make sure you're up to date:

    git pull

Now do the release:

    pdm run mike deploy --push --update-aliases <version> latest

That's it! New documentation should new be visible at [docs.recap.cloud](https://docs.recap.cloud).