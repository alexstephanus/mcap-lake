FROM duckdb/duckdb:1.5.3

# We bake these extensions in during image creation so we don't need internet
# access to spin up a new container.
RUN ["/duckdb", "-c", "INSTALL iceberg; INSTALL httpfs; INSTALL avro;"]

ENTRYPOINT ["/duckdb", "-init", "/lake_init.sql"]
