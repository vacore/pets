# rustdb
A prototype of an in-memory database storage system in Rust

### Server
A bunch of emulated writers are writing to the DB simultaneously

### Client
A remote application with fast access to the DB according to queries.
For now a few queries are supported: Home/End, PageUp/PageDown, Up/Down, Knob selection (sort of Remote Excel)

### Concepts
- Record: a user data, with custom types and length (i.e. a row in a RDBMS table)
- Index: internal data structure, allowing for fast search and retreival
- Table: holds various metadata
