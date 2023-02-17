Though Recap's CLI can run without any configuration, you might want to configure Recap. Recap uses Pydantic's [BaseSettings](https://docs.pydantic.dev/usage/settings/) class for its configuration system.

## Configs

See Recap's [config.py](https://github.com/recap-cloud/recap/blob/main/recap/config.py) for all available configuration parameters.

Commonly set environment variables include:

```bash
RECAP_STORAGE_SETTINGS__URL=http://localhost:8000/storage
RECAP_LOGGING_CONFIG_FILE=/tmp/logging.toml
```

!!! note

	Note the double-underscore (_dunder_) in the `URL` environment variable. This is a common way to set nested dictionary and object values in Pydantic's `BaseSettings` classes. You can also set JSON objects like `RECAP_STORAGE_SETTINGS='{"url": "http://localhost:8000/storage"}'`. See Pydantic's [settings management](https://docs.pydantic.dev/usage/settings/) page for more information.

## Dotenv

Recap supports [.env](https://www.dotenv.org) files to manage environment variables. Simply create a `.env` in your current working directory and use Recap as usual. Pydantic handles the rest.

## Home

RECAP_HOME defines where Recap looks for storage and secret files. By default, RECAP_HOME is set to `~/.recap`.

## Secrets

You can set environment variables with secrets in them using Pydantic's [secret handling mechanism](https://docs.pydantic.dev/usage/settings/#secret-support). By default, Recap looks for secrets in `$RECAP_HOME/.secrets`.
