# Releasing Recap

Recap is released using the `publish.yaml` GitHub Action workflow. Simply create a release in GitHub and the workflow will run automatically. The Github release should create a new tag in the format `x.y.z`. which should match the version in the pyproject.toml file. `publish.yaml` will automatically:

1. Run tests
2. Publish Recap to PyPI
3. Publish Recap to Docker Hub
4. Increment the patch version in pyproject.toml
