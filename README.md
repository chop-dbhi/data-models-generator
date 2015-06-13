# Data Models Generator

## Docker

*This is recommeneded to bypass having to install Oracle client libraries and such.*

Run the script to output the usage information.

```bash
docker run -it --rm dbhi/data-models-generator
```

## Examples

### SQL

The minimum arguments are model, version, engine, and database.

```bash
docker run -it --rm dbhi/data-models-generator sql omop v4 postgresql omop_v4_db
```

### REDCap

To see the usage, run:

```bash
docker run -it --rm dbhi/data-models-generator redcap help
```

REDCap data dictionary.

```bash
docker run -it --rm dbhi/data-models-generator redcap csv myproject v1 /path/to/data_dictionary.csv
```

REDCap API.

```bash
docker run -it --rm dbhi/data-models-generator redcap api myproject v1 https://example.com/api/ ABC123
```

REDCap database.

```bash
docker run -it --rm dbhi/data-models-generator redcap db myproject v1 myproject --host=example.com
```
