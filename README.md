# Data Models Generator

## Docker

*This is recommeneded to bypass having to install Oracle client libraries and such.*

Run the script to output the usage information.

```
docker run -it --rm dbhi/data-models-generator
```

## Example

The minimum arguments are model, version, engine, database.

```
docker run -it --rm dbhi/data-models-generator omop v4 postgresql omop_v4_db
```
