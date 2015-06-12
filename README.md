# Data Models Generator

## Docker

*This is recommeneded to bypass having to install Oracle client libraries and such.*

Run the script to output the usage information.

```
docker run -it --rm dbhi/data-models-generator
```

## Examples

### SQL

The minimum arguments are model, version, engine, and database.

```
docker run -it --rm dbhi/data-models-generator sql omop v4 postgresql omop_v4_db
```

### REDCap

The minimum arguments are model, version, and a path or URL and token.

CSV data dictionary.

```
docker run -it --rm dbhi/data-models-generator redcap myproject /path/to/data_dictionary.csv
```

REDCap API with URL and token.

```
docker run -it --rm dbhi/data-models-generator redcap myproject https://example.com/api/ ABC123
```
