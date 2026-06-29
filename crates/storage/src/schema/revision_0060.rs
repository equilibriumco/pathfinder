use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Removing not null constraint on definitions in the casm_definitions table");

    tx.execute_batch(
        r"
        CREATE TABLE new_casm_definitions (
            hash                BLOB    PRIMARY KEY NOT NULL,
            compiled_class_hash BLOB    NOT NULL,
            definition          BLOB,
            FOREIGN KEY(hash) REFERENCES class_definitions(hash) ON DELETE CASCADE
        );
        INSERT INTO new_casm_definitions (hash, compiled_class_hash, definition) SELECT hash, compiled_class_hash, definition FROM casm_definitions;
        DROP TABLE casm_definitions;
        ALTER TABLE new_casm_definitions RENAME TO casm_definitions;
        CREATE INDEX casm_definitions_compiled_class_hash ON casm_definitions(compiled_class_hash);",
    )
    .context("Removing not null constraint on definitions in the casm_definitions table")?;

    Ok(())
}
