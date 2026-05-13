use anyhow::Context;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Migrating CASM definitions");

    tx.execute_batch(
        r"
        CREATE TABLE casm_definitions_new(
            hash                BLOB    PRIMARY KEY NOT NULL,
            definition          BLOB,
            FOREIGN KEY(hash) REFERENCES class_definitions(hash) ON DELETE CASCADE
        );
        CREATE TABLE casm_class_hashes(
            hash                BLOB    NOT NULL,
            block_number        INTEGER,
            compiled_class_hash BLOB    NOT NULL,
            FOREIGN KEY(hash) REFERENCES class_definitions(hash) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX casm_class_hashes_hash_block_number
            ON casm_class_hashes(hash, block_number);
        ",
    )
    .context("Creating new casm_definitions and casm_class_hashes tables")?;

    tx.execute_batch(
        r"
        INSERT INTO
            casm_class_hashes(hash, block_number, compiled_class_hash)
        SELECT
            cd.hash AS hash,
            cd.block_number AS block_number,
            cdm.compiled_class_hash AS compiled_class_hash
        FROM
            class_definitions cd
        INNER JOIN
            casm_definitions cdm ON cd.hash = cdm.hash;
        ",
    )
    .context("Transferring CASM class hashes to new casm_class_hashes table")?;

    tx.execute_batch(
        r"
        INSERT INTO
            casm_definitions_new(hash, definition)
        SELECT
            hash,
            definition
        FROM
            casm_definitions;
        ",
    )
    .context("Transferring data to new casm_definitions table")?;

    tx.execute_batch(
        r"
        DROP TABLE casm_definitions;
        ALTER TABLE casm_definitions_new RENAME TO casm_definitions;
        ",
    )
    .context("Replacing old casm_definitions with the new table")?;

    Ok(())
}
