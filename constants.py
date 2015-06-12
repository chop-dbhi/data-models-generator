MODEL_COLUMNS = (
    'model',
    'version',
    'label',
    'url',
    'description',
)

TABLE_COLUMNS = (
    'model',
    'version',
    'table',
    'description',
)

FIELD_COLUMNS = (
    'model',
    'version',
    'table',
    'field',
    'label',
    'description',
)

SCHEMA_COLUMNS = (
    'model',
    'version',
    'table',
    'field',
    'type',
    'length',
    'precision',
    'scale',
    'default',
)

CONSTRAINT_COLUMNS = (
    'model',
    'version',
    'table',
    'field',
    'type',
    'name',
)

INDEX_COLUMNS = (
    'model',
    'version',
    'table',
    'field',
    'name',
    'order',
)


REFERENCE_COLUMNS = (
    'model',
    'version',
    'table',
    'field',
    'ref_table',
    'ref_field',
    'name',
)
