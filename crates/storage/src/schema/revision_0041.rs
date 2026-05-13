use anyhow::Context;

/// This migration replaces the hash-index'd tree tables with integer ones and
/// other changes related to this.
///
/// Because this is the first migration post-base, we are guaranteed that there
/// is no data to migrate.
pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    drop_table(tx, "tree_class").context("Dropping tree_class table")?;
    drop_table(tx, "tree_global").context("Dropping tree_global table")?;
    drop_table(tx, "tree_contracts").context("Dropping tree_contracts table")?;

    create_trie_table(tx, "trie_class").context("Creating trie_class table")?;
    create_trie_table(tx, "trie_storage").context("Creating trie_storage table")?;
    create_trie_table(tx, "trie_contracts").context("Creating trie_contracts table")?;

    create_roots_table(tx, "class_roots").context("Creating class_roots table")?;
    create_roots_table(tx, "storage_roots").context("Creating storage_roots table")?;

    tx.execute(
        r"CREATE TABLE contract_roots (
    block_number     INTEGER NOT NULL,
    contract_address BLOB NOT NULL,
    root_index       INTEGER
)",
        [],
    )
    .context("Creating contract_roots table")?;

    tx.execute(
        "CREATE INDEX contract_roots_address_block_number ON contract_roots(contract_address, \
         block_number)",
        [],
    )
    .context("Creating contract_roots_address_block_number index")?;

    // Redo the class commitment leaf table. This is safe to do without migrating
    // the data since this is the first migration.
    tx.execute("DROP TABLE class_commitment_leaves", [])
        .context("Dropping class_commitment_leaves table")?;
    tx.execute(
        r"CREATE TABLE class_commitment_leaves (
    block_number INTEGER NOT NULL,
    leaf BLOB NOT NULL,
    casm BLOB NOT NULL
)",
        [],
    )
    .context("Creating new class_commitment_leaves table")?;

    tx.execute(
        "CREATE INDEX class_commitment_leaves_casm ON class_commitment_leaves(casm)",
        [],
    )
    .context("Creating class_commitment_leaves_casm index")?;

    // Redo the contract state hash table. We already store nonce, root and class
    // hashes in separate tables, so we only need the state hash now.
    tx.execute("DROP TABLE contract_states", [])
        .context("Dropping contract_states table")?;
    tx.execute(
        r"CREATE TABLE contract_state_hashes (
    block_number     INTEGER NOT NULL,
    contract_address BLOB NOT NULL,
    state_hash       BLOB NOT NULL
)",
        [],
    )
    .context("Creating contract_state_hashes table")?;

    tx.execute(
        "CREATE INDEX contract_state_hashes_address ON contract_state_hashes(contract_address)",
        [],
    )
    .context("Creating contract_state_hashes_address index")?;

    Ok(())
}

fn drop_table(tx: &rusqlite::Transaction<'_>, table: &'static str) -> anyhow::Result<()> {
    tx.execute(&format!("DROP TABLE {table}"), [])?;
    Ok(())
}

fn create_trie_table(tx: &rusqlite::Transaction<'_>, table: &'static str) -> anyhow::Result<()> {
    tx.execute(
        &format!(
            r"CREATE TABLE {table} (
    idx INTEGER PRIMARY KEY,
    hash BLOB NOT NULL,
    data BLOB
)"
        ),
        [],
    )?;
    Ok(())
}

fn create_roots_table(tx: &rusqlite::Transaction<'_>, table: &'static str) -> anyhow::Result<()> {
    tx.execute(
        &format!(
            r"CREATE TABLE {table} (
    block_number INTEGER PRIMARY KEY,
    root_index   INTEGER
)"
        ),
        [],
    )?;
    Ok(())
}
