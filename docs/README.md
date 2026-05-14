# Espresso Operation Guides

This directory contains detailed configuration guides for espresso workloads across Alternator and CQL backends.

## Core Reference

- [Profile Configuration Reference](config-reference.md)
- [Profile Cookbook](cookbook.md)
- [CQL Backend Guide](cql-backend.md)

## Operation Docs

- [PutItem](operations-put-item.md)
- [GetItem](operations-get-item.md)
- [BatchWriteItem](operations-batch-write-item.md)
- [BatchGetItem](operations-batch-get-item.md)
- [Query](operations-query.md)
- [Scan](operations-scan.md)
- [UpdateItem](operations-update-item.md)
- [DeleteItem](operations-delete-item.md)

## Notes

- Examples use the same profile structure used in `espresso_profiles/key_value.yaml`.
- Operation templates are declarative; espresso generates internal expressions as needed.
- All examples are intended as full, copyable profiles that can be adapted per environment.
