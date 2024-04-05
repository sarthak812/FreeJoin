# DuckDB Commands for TPCH

```console 
INSTALL tpch;
```

If the file does not exist it will be created
```console
.open my_tpch_database.duckdb
```

```console 
LOAD tpch;
```

```console 
CALL dbgen(sf=1);
```

```console 
show tables;
```
