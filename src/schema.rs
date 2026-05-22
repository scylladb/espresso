//! Schema management — create or recreate DynamoDB tables driven by the
//! profile's `tables` and `schema` sections.

use anyhow::{Context, Result};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType, TableStatus,
};
use scylla::client::session::Session;
use tracing::{info, warn};

use crate::config::{
    ApplyToTables, AttributeType, BillingMode as CfgBilling, SchemaConfig, TableDef,
};

/// Apply schema actions (create / delete) for all tables covered by the
/// profile's `schema` section.
pub async fn apply(client: &Client, tables: &[TableDef], schema: &SchemaConfig) -> Result<()> {
    let target_tables: Vec<&TableDef> = match &schema.apply_to_tables {
        ApplyToTables::All(_) => tables.iter().collect(),
        ApplyToTables::Named(names) => tables.iter().filter(|t| names.contains(&t.name)).collect(),
    };

    for table in target_tables {
        apply_one(client, table, schema).await?;
    }
    Ok(())
}

async fn apply_one(client: &Client, table: &TableDef, schema: &SchemaConfig) -> Result<()> {
    let exists = table_exists(client, &table.name).await?;

    if exists && schema.delete_if_exists {
        info!("Deleting existing table '{}'", table.name);
        client
            .delete_table()
            .table_name(&table.name)
            .send()
            .await
            .with_context(|| format!("failed to delete table '{}'", table.name))?;
        wait_for_delete(client, &table.name).await?;
        create_table(client, table).await?;
    } else if !exists && schema.create_if_missing {
        create_table(client, table).await?;
    } else if !exists && !schema.create_if_missing {
        warn!(
            "Table '{}' does not exist and create_if_missing=false — proceeding anyway",
            table.name
        );
    } else {
        info!("Table '{}' already exists, skipping creation", table.name);
    }

    Ok(())
}

async fn create_table(client: &Client, table: &TableDef) -> Result<()> {
    info!("Creating table '{}'", table.name);

    // Build AttributeDefinitions — only key attributes need to be declared.
    let key_attrs: Vec<AttributeDefinition> = {
        let pk = &table.key_schema.partition_key;
        let mut defs = vec![
            AttributeDefinition::builder()
                .attribute_name(&pk.name)
                .attribute_type(to_scalar_type(pk.attr_type))
                .build()
                .context("failed to build partition key attribute definition")?,
        ];
        if let Some(sk) = &table.key_schema.sort_key {
            defs.push(
                AttributeDefinition::builder()
                    .attribute_name(&sk.name)
                    .attribute_type(to_scalar_type(sk.attr_type))
                    .build()
                    .context("failed to build sort key attribute definition")?,
            );
        }
        defs
    };

    // Build KeySchema
    let pk = &table.key_schema.partition_key;
    let mut key_schema = vec![
        KeySchemaElement::builder()
            .attribute_name(&pk.name)
            .key_type(KeyType::Hash)
            .build()
            .context("failed to build partition key schema element")?,
    ];
    if let Some(sk) = &table.key_schema.sort_key {
        key_schema.push(
            KeySchemaElement::builder()
                .attribute_name(&sk.name)
                .key_type(KeyType::Range)
                .build()
                .context("failed to build sort key schema element")?,
        );
    }

    let billing = match table.billing_mode {
        CfgBilling::PayPerRequest => BillingMode::PayPerRequest,
        CfgBilling::Provisioned => BillingMode::Provisioned,
    };

    let mut req = client
        .create_table()
        .table_name(&table.name)
        .billing_mode(billing);

    for def in key_attrs {
        req = req.attribute_definitions(def);
    }
    for ks in key_schema {
        req = req.key_schema(ks);
    }

    req.send()
        .await
        .with_context(|| format!("failed to create table '{}'", table.name))?;

    wait_for_active(client, &table.name).await?;
    info!("Table '{}' is ACTIVE", table.name);
    Ok(())
}

// ── Polling helpers ───────────────────────────────────────────────────────────

async fn table_exists(client: &Client, name: &str) -> Result<bool> {
    match client.describe_table().table_name(name).send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            use aws_sdk_dynamodb::error::SdkError;
            if let SdkError::ServiceError(ref se) = e {
                if se.err().is_resource_not_found_exception() {
                    return Ok(false);
                }
            }
            Err(e).context(format!("failed to describe table '{name}'"))
        }
    }
}

async fn wait_for_active(client: &Client, name: &str) -> Result<()> {
    loop {
        let resp = client
            .describe_table()
            .table_name(name)
            .send()
            .await
            .with_context(|| format!("describe_table failed while waiting for '{name}'"))?;

        let status = resp
            .table()
            .and_then(|t| t.table_status())
            .cloned()
            .unwrap_or(TableStatus::Creating);

        if status == TableStatus::Active {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

async fn wait_for_delete(client: &Client, name: &str) -> Result<()> {
    loop {
        match client.describe_table().table_name(name).send().await {
            Err(e) => {
                use aws_sdk_dynamodb::error::SdkError;
                if let SdkError::ServiceError(ref se) = e {
                    if se.err().is_resource_not_found_exception() {
                        return Ok(());
                    }
                }
                return Err(e).context(format!(
                    "describe_table failed while waiting for '{name}' deletion"
                ));
            }
            Ok(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

// ── Type conversion ───────────────────────────────────────────────────────────

fn to_scalar_type(t: AttributeType) -> ScalarAttributeType {
    if t.is_text_like() {
        ScalarAttributeType::S
    } else if t.is_blob_like() {
        ScalarAttributeType::B
    } else {
        ScalarAttributeType::N
    }
}

/// Apply schema actions for CQL backend.
///
/// CQL backend creates the keyspace (if needed) and tables based on profile key
/// schema. Non-key attributes are created as regular columns.
pub async fn apply_cql(
    session: &Session,
    keyspace: &str,
    cql_cfg: &crate::config::CqlConfig,
    tables: &[TableDef],
    schema: &SchemaConfig,
) -> Result<()> {
    ensure_keyspace(session, keyspace, cql_cfg).await?;

    let target_tables: Vec<&TableDef> = match &schema.apply_to_tables {
        ApplyToTables::All(_) => tables.iter().collect(),
        ApplyToTables::Named(names) => tables.iter().filter(|t| names.contains(&t.name)).collect(),
    };

    for table in target_tables {
        let fqtn = format!("{keyspace}.{}", table.name);
        if schema.delete_if_exists {
            let drop = format!("DROP TABLE IF EXISTS {fqtn}");
            session
                .query_unpaged(drop, ())
                .await
                .with_context(|| format!("failed to drop CQL table '{fqtn}'"))?;
        }

        if schema.create_if_missing || schema.delete_if_exists {
            let create = build_create_table_cql(keyspace, table);
            session
                .query_unpaged(create.clone(), ())
                .await
                .with_context(|| format!("failed to create CQL table '{}'", table.name))?;
            info!("CQL table ready: {fqtn}");
        } else {
            info!(
                "CQL schema create disabled for table '{}', skipping",
                table.name
            );
        }
    }

    Ok(())
}

async fn ensure_keyspace(
    session: &Session,
    keyspace: &str,
    cql_cfg: &crate::config::CqlConfig,
) -> Result<()> {
    let mut cql = format!(
        "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {}}}",
        cql_cfg.replication_factor
    );

    if let Some(enabled) = cql_cfg.tablets_enabled {
        if let Some(initial) = cql_cfg.tablets_initial {
            cql.push_str(&format!(
                " AND tablets = {{'enabled': {}, 'initial': {}}}",
                if enabled { "true" } else { "false" },
                initial
            ));
        } else {
            cql.push_str(&format!(
                " AND tablets = {{'enabled': {}}}",
                if enabled { "true" } else { "false" }
            ));
        }
    }

    session
        .query_unpaged(cql, ())
        .await
        .with_context(|| format!("failed to create keyspace '{keyspace}'"))?;
    Ok(())
}

fn build_create_table_cql(keyspace: &str, table: &TableDef) -> String {
    let mut cols = Vec::new();
    for attr in &table.attributes {
        cols.push(format!("{} {}", attr.name, to_cql_type(attr.attr_type)));
    }

    let pk = &table.key_schema.partition_key.name;
    let primary = if let Some(sk) = &table.key_schema.sort_key {
        format!("PRIMARY KEY (({}), {})", pk, sk.name)
    } else {
        format!("PRIMARY KEY (({}))", pk)
    };

    format!(
        "CREATE TABLE IF NOT EXISTS {keyspace}.{} ({}, {})",
        table.name,
        cols.join(", "),
        primary
    )
}

fn to_cql_type(t: AttributeType) -> &'static str {
    t.to_cql_type()
}
