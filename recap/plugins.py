import typer
import sys


if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


CLI_PLUGIN_GROUP = 'recap.plugins.cli'


def load_cli_plugins(app: typer.Typer) -> typer.Typer:
    cli_plugins = entry_points(group=CLI_PLUGIN_GROUP)

    for cli_plugin in cli_plugins:
        cli_plugin_name = cli_plugin.name
        cli_plugin_typer = cli_plugin.load()
        # If the plugin has a single command, then put it directly into the
        # current Typer app. If the plugin has multiple commands, then treat it
        # as a command group, and add it as such.
        # TODO Shouldn't need to do this, but Typer has a bug.
        # https://github.com/tiangolo/typer/issues/119
        if len(cli_plugin_typer.registered_commands) == 1:
            app.command(cli_plugin_name)(cli_plugin_typer.registered_commands[0].callback)
        else:
            app.add_typer(cli_plugin_typer, name=cli_plugin_name)

    return app
