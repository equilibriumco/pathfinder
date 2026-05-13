//! Prior to this migration, [state update](crate::connection::state_update)
//! related tables have had FOREIGN KEY references with the ON DELETE CASCADE
//! action to the `canonical_blocks` table. The goal was to make purging blocks
//! easier by having a cascading delete effect on these tables when a row from
//! `canonical_blocks` is deleted.
//!
//! With the introduction of [blockchain pruning](crate::connection::pruning), a
//! need has arisen to delete block related table entries (including
//! `canonical_blocks`) independently of the state update tables. This was, of
//! course, not possible due to the aforementioned FOREIGN KEY references
//! leading to multiple workarounds and unnecessary complexity.
//!
//! This migration removes the FOREIGN KEY references to `canonical_blocks` from
//! the state update tables.

use std::ops::Deref;

use anyhow::Context;

const LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

struct TableUpdate {
    table_name: &'static str,
    create_table_stmt: &'static str,
    transfer_stmt: &'static str,
    drop_rename_create_index_stmt_batch: &'static str,
}

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Dropping FOREIGN KEY references from several state update related tables");

    let table_updates = [
        TableUpdate {
            table_name: "block_signatures",
            create_table_stmt: r"
                CREATE TABLE block_signatures_new(
                    block_number INTEGER REFERENCES block_headers(number) ON DELETE CASCADE,
                    signature_r  BLOB NOT NULL,
                    signature_s  BLOB NOT NULL
                );
            ",
            transfer_stmt: "INSERT INTO block_signatures_new SELECT * FROM block_signatures",
            drop_rename_create_index_stmt_batch: r"
                DROP TABLE block_signatures;
                ALTER TABLE block_signatures_new RENAME TO block_signatures;
                CREATE UNIQUE INDEX block_signatures_block_number 
                    ON block_signatures(block_number);
            ",
        },
        TableUpdate {
            table_name: "class_definitions",
            create_table_stmt: r"
                CREATE TABLE class_definitions_new(
                    hash         BLOB PRIMARY KEY,
                    definition   BLOB,
                    block_number INTEGER
                );
            ",
            transfer_stmt: "INSERT INTO class_definitions_new SELECT * FROM class_definitions",
            drop_rename_create_index_stmt_batch: r"
                DROP TABLE class_definitions;
                ALTER TABLE class_definitions_new RENAME TO class_definitions;
                CREATE INDEX class_definitions_block_number 
                    ON class_definitions(block_number);
            ",
        },
        // This table definition does not change but its content will be lost after the
        // `class_definitions` table gets dropped.
        TableUpdate {
            table_name: "casm_definitions",
            // Setting the FOREIGN KEY to the `class_definitions_new` table, the reference target
            // will be renamed when the `class_definitions_new` table gets renamed to
            // `class_definitions`.
            create_table_stmt: r"
                CREATE TABLE casm_definitions_new(
                    hash                BLOB    PRIMARY KEY NOT NULL,
                    compiled_class_hash BLOB    NOT NULL,
                    definition          BLOB,
                    FOREIGN KEY(hash) REFERENCES class_definitions_new(hash) ON DELETE CASCADE
                );
            ",
            transfer_stmt: "INSERT INTO casm_definitions_new SELECT * FROM casm_definitions",
            drop_rename_create_index_stmt_batch: r"
                DROP TABLE casm_definitions;
                ALTER TABLE casm_definitions_new RENAME TO casm_definitions;
                CREATE INDEX casm_definitions_compiled_class_hash ON casm_definitions(compiled_class_hash);
            ",
        },
        TableUpdate {
            table_name: "contract_updates",
            create_table_stmt: r"
                CREATE TABLE contract_updates_new(
                    block_number     INTEGER NOT NULL,
                    contract_address BLOB NOT NULL,
                    class_hash       BLOB NOT NULL
                );
            ",
            transfer_stmt: "INSERT INTO contract_updates_new SELECT * FROM contract_updates",
            drop_rename_create_index_stmt_batch: r"
                DROP TABLE contract_updates;
                ALTER TABLE contract_updates_new RENAME TO contract_updates;
                CREATE INDEX contract_updates_block_number 
                    ON contract_updates(block_number);
                CREATE INDEX contract_updates_address_block_number 
                    ON contract_updates(contract_address, block_number);
            ",
        },
        TableUpdate {
            table_name: "nonce_updates",
            create_table_stmt: r"
                CREATE TABLE nonce_updates_new(
                    block_number        INTEGER NOT NULL,
                    contract_address_id INTEGER NOT NULL REFERENCES contract_addresses(id),
                    nonce               BLOB NOT NULL
                );
            ",
            transfer_stmt: "INSERT INTO nonce_updates_new SELECT * FROM nonce_updates",
            drop_rename_create_index_stmt_batch: r"
                DROP TABLE nonce_updates;
                ALTER TABLE nonce_updates_new RENAME TO nonce_updates;
                CREATE INDEX nonce_updates_block_number 
                    ON nonce_updates(block_number);
                CREATE INDEX nonce_updates_contract_address_id_block_number 
                    ON nonce_updates(contract_address_id, block_number);
            ",
        },
        TableUpdate {
            table_name: "storage_updates",
            create_table_stmt: r"
                CREATE TABLE storage_updates_new(
                    block_number        INTEGER NOT NULL,
                    contract_address_id INTEGER NOT NULL REFERENCES contract_addresses(id),
                    storage_address_id  INTEGER NOT NULL REFERENCES storage_addresses(id),
                    storage_value       BLOB NOT NULL
                );
            ",
            transfer_stmt: "INSERT INTO storage_updates_new SELECT * FROM storage_updates",
            drop_rename_create_index_stmt_batch: r"
                DROP TABLE storage_updates;
                ALTER TABLE storage_updates_new RENAME TO storage_updates;
                CREATE INDEX storage_updates_block_number 
                    ON storage_updates(block_number);
                CREATE INDEX storage_updates_contract_address_id_storage_address_id_block_number 
                    ON storage_updates(contract_address_id, storage_address_id, block_number);
            ",
        },
    ];

    let mut last_logged = std::time::Instant::now();
    tx.deref().progress_handler(
        // Shooting for about a second with this.
        50_000_000,
        Some(move || {
            if last_logged.elapsed() > LOG_INTERVAL {
                tracing::info!("DB operation in progress");
                last_logged = std::time::Instant::now();
            }

            false
        }),
    );

    tracing::info!("Creating new tables and transferring data");

    for update in &table_updates {
        let table = &update.table_name;
        tx.execute(update.create_table_stmt, [])
            .with_context(|| format!("Creating {table}_new table"))?;
        tx.execute(update.transfer_stmt, [])
            .with_context(|| format!("Transferring data from {table} to {table}_new",))?;
    }

    tracing::info!("Dropping old tables, renaming new ones and re-creating indices");

    for update in &table_updates {
        tx.execute_batch(update.drop_rename_create_index_stmt_batch)
            .with_context(|| {
                format!(
                    "Dropping {table} table, renaming {table}_new to {table} and re-creating \
                     indices",
                    table = update.table_name
                )
            })?;
    }

    tracing::info!("Dropping `canonical_blocks` table");

    tx.execute("DROP TABLE canonical_blocks", [])
        .context("Dropping canonical_blocks table")?;

    Ok(())
}
