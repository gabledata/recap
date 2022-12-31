Recap uses Python's standard [logging](https://docs.python.org/3/library/logging.html) library. Logs are printed to standard out using Rich's [logging handler](https://rich.readthedocs.io/en/stable/logging.html) by default.

## Customizing

You can customize Recap's log output. Set the `logging.config.path` value in your `settings.toml` file to point at a [TOML](https://toml.io) file that conforms to Python's [dictConfig](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema) schema.

```toml
version = 1
disable_existing_loggers = true
formatters.standard.format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

[handlers.default]
formatter = "standard"
class = "rich.logging.RichHandler"
show_time = false
show_level = false
show_path = false

[loggers.""]
handlers = ['default']
level = "WARNING"
propagate = false

[loggers.recap]
handlers = ['default']
level = "INFO"
propagate = false

[loggers.uvicorn]
handlers = ['default']
level = "INFO"
propagate = false
```