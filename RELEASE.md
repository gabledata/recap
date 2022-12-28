# Releasing Recap

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