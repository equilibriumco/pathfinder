use anyhow::Context;
use pathfinder_casm_hashes::get_precomputed_casm_v2_hash;
use pathfinder_common::class_definition::SerializedCasmDefinition;
use pathfinder_common::ClassHash;
use pathfinder_crypto::Felt;
use rayon::prelude::*;

use crate::prelude::*;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Computing CASM v2 class hashes");

    tx.execute_batch(
        r"
        CREATE TABLE casm_class_hashes_v2(
            hash                BLOB    PRIMARY KEY NOT NULL,
            compiled_class_hash BLOB    NOT NULL,
            FOREIGN KEY(hash) REFERENCES class_definitions(hash) ON DELETE CASCADE
        );
        ",
    )
    .context("Creating new casm_class_hashes_v2 tables")?;

    let mut read_casm_classes_stmt: rusqlite::Statement<'_> =
        tx.prepare("SELECT hash, definition FROM casm_definitions")?;
    let casm_definitions: Vec<_> = read_casm_classes_stmt
        .query_map([], |row| {
            let class_hash: Vec<u8> = row.get(0)?;
            let class_hash = ClassHash(
                Felt::from_be_slice(&class_hash)
                    .map_err(|e| rusqlite::types::FromSqlError::Other(e.into()))?,
            );

            match get_precomputed_casm_v2_hash(&class_hash) {
                Some(casm_hash) => Ok((class_hash, Some(*casm_hash), None)),
                None => {
                    let definition: Vec<u8> = row.get(1)?;
                    Ok((class_hash, None, Some(definition)))
                }
            }
        })?
        .collect();

    let casm_v2_hashes: Vec<_> = casm_definitions
        .into_par_iter()
        .map(|result| {
            let (class_hash, casm_hash, definition) = result.unwrap();
            match casm_hash {
                Some(casm_hash) => (class_hash, casm_hash),
                None => {
                    let definition = definition.expect("Definition must be present");
                    let definition = zstd::decode_all(definition.as_slice())
                        .map_err(|e| rusqlite::types::FromSqlError::Other(e.into()))
                        .unwrap();
                    let computed_hash = pathfinder_compiler::casm_class_hash_v2(
                        &SerializedCasmDefinition::from_vec(definition),
                    )
                    .unwrap();
                    (class_hash, computed_hash)
                }
            }
        })
        .collect();

    tracing::info!(number_of_class_hashes=%casm_v2_hashes.len(), "Inserting CASM v2 class hashes into the database");

    let mut insert_casm_v2_hashes_stmt: rusqlite::Statement<'_> =
        tx.prepare("INSERT INTO casm_class_hashes_v2(hash, compiled_class_hash) VALUES (?, ?)")?;
    for (class_hash, casm_v2_hash) in casm_v2_hashes {
        insert_casm_v2_hashes_stmt.execute(params![&class_hash, &casm_v2_hash])?;
    }

    Ok(())
}
