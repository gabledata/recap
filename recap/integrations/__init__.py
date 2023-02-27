# Load all integrations into the registry

try:
    import recap.integrations.bigquery
except:
    # Skip Google if dependencies aren't installed.
    pass
import recap.integrations.fsspec
import recap.integrations.sqlalchemy
