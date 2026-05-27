use anyhow::Context;
use futures::StreamExt;
use pathfinder_common::ClassHash;
use pathfinder_storage::Storage;
use starknet_gateway_client::GatewayApi;

use super::class::{download_class, DownloadedClass};

pub async fn repair_missing_class_definitions<S: GatewayApi + Clone + Send + 'static>(
    storage: Storage,
    sequencer: S,
    compiler_resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    fetch_casm_from_fgw: bool,
) -> anyhow::Result<()> {
    repair_missing_class_definitions_with(storage, move |hash| {
        let sequencer = sequencer.clone();
        async move {
            download_class(
                &sequencer,
                hash,
                compiler_resource_limits,
                blockifier_libfuncs,
                fetch_casm_from_fgw,
            )
            .await
        }
    })
    .await
}

/// Inner implementation that accepts an injectable download function for
/// tests, allowing us to pass a fake that returns known [`DownloadedClass`]
/// values without using the sequencer or running hash computation.
async fn repair_missing_class_definitions_with<F, Fut>(
    storage: Storage,
    download: F,
) -> anyhow::Result<()>
where
    F: Fn(ClassHash) -> Fut + Clone,
    Fut: std::future::Future<Output = anyhow::Result<DownloadedClass>>,
{
    let missing = tokio::task::spawn_blocking({
        let storage = storage.clone();
        move || {
            let mut db = storage
                .connection()
                .context("Creating database connection")?;
            let tx = db.transaction().context("Creating transaction")?;
            tx.class_hashes_with_missing_definitions()
                .context("Querying missing class definitions")
        }
    })
    .await
    .context("Joining database task")??;

    if missing.is_empty() {
        tracing::debug!("No missing class definitions to repair");
        return Ok(());
    }

    let total = missing.len();
    tracing::info!(count = total, "Repairing missing class definitions");

    let mut repaired = 0usize;
    let mut failed = 0usize;

    let results = futures::stream::iter(missing)
        .map(|hash| {
            let download = download.clone();
            async move { (hash, download(hash).await) }
        })
        .buffer_unordered(4);

    tokio::pin!(results);

    while let Some((declared_hash, result)) = results.next().await {
        match result {
            Err(e) => {
                tracing::warn!(hash=%declared_hash, error=%e, "Failed to download class definition for repair");
                failed += 1;
            }
            Ok(downloaded) => {
                let store_result = tokio::task::spawn_blocking({
                    let storage = storage.clone();
                    move || store_repaired_class(&storage, declared_hash, downloaded)
                })
                .await
                .unwrap_or_else(|e| Err(anyhow::anyhow!(e)));

                match store_result {
                    Err(e) => {
                        tracing::warn!(hash=%declared_hash, error=%e, "Failed to store repaired class definition");
                        failed += 1;
                    }
                    Ok(()) => {
                        repaired += 1;
                    }
                }
            }
        }
    }

    if failed == 0 {
        tracing::info!(total, repaired, "Finished repairing class definitions");
    } else {
        tracing::warn!(
            total,
            repaired,
            failed,
            "Finished repairing class definitions"
        );
    }

    Ok(())
}

fn store_repaired_class(
    storage: &Storage,
    declared_hash: ClassHash,
    downloaded: DownloadedClass,
) -> anyhow::Result<()> {
    let mut db = storage
        .connection()
        .context("Creating database connection")?;
    let tx = db.transaction().context("Creating transaction")?;

    match downloaded {
        DownloadedClass::Cairo { definition, .. } => {
            tx.update_cairo_class_definition(declared_hash, &definition)
                .context("Storing repaired Cairo class definition")?;
        }
        DownloadedClass::Sierra {
            sierra_definition,
            casm_definition,
            casm_hash_v2,
            ..
        } => {
            tx.update_sierra_class_definition(
                &pathfinder_common::SierraHash(declared_hash.0),
                &sierra_definition,
                &casm_definition,
                &casm_hash_v2,
            )
            .context("Storing repaired Sierra class definition")?;
        }
    }

    tx.commit()
        .context("Committing repaired class definition")
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use pathfinder_common::class_definition::{
        SerializedCairoDefinition,
        SerializedOpaqueClassDefinition,
    };
    use pathfinder_common::state_update::StateUpdateData;
    use pathfinder_common::BlockNumber;
    use pathfinder_storage::StorageBuilder;

    use super::*;

    fn storage() -> Storage {
        StorageBuilder::in_memory().unwrap()
    }

    fn insert_placeholder(storage: &Storage, hash: ClassHash) {
        let mut db = storage.connection().unwrap();
        let tx = db.transaction().unwrap();
        tx.insert_state_update_data(
            BlockNumber::GENESIS,
            &StateUpdateData {
                declared_cairo_classes: HashSet::from([hash]),
                ..Default::default()
            },
        )
        .unwrap();
        tx.commit().unwrap();
    }

    #[tokio::test]
    async fn nothing_to_repair() {
        let storage = storage();

        repair_missing_class_definitions_with(storage, |_hash| async {
            panic!("download should not be called when there are no missing definitions")
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cairo_repair() {
        let storage = storage();
        let hash = ClassHash(pathfinder_crypto::Felt::from_hex_str("0xdeadbeef").unwrap());
        let definition = SerializedCairoDefinition::from_slice(b"cairo class definition bytes");

        insert_placeholder(&storage, hash);

        repair_missing_class_definitions_with(storage.clone(), {
            let definition = definition.clone();
            move |requested| {
                let definition = definition.clone();
                async move {
                    Ok(DownloadedClass::Cairo {
                        hash: requested,
                        definition,
                    })
                }
            }
        })
        .await
        .unwrap();

        let mut db = storage.connection().unwrap();
        let tx = db.transaction().unwrap();
        let stored = tx.class_definition(hash).unwrap();
        assert_eq!(
            stored,
            Some(SerializedOpaqueClassDefinition::from(definition))
        );
    }

    /// Cairo 0 classes can have a mismatch between the declared hash and the
    /// hash computed from the downloaded bytes. The repair must store the
    /// definition under the declared hash regardless.
    #[tokio::test]
    async fn cairo_repair_hash_mismatch() {
        let storage = storage();
        let declared = ClassHash(pathfinder_crypto::Felt::from_hex_str("0xaaaa").unwrap());
        let computed = ClassHash(pathfinder_crypto::Felt::from_hex_str("0xbbbb").unwrap());
        let definition = SerializedCairoDefinition::from_slice(b"cairo class definition bytes");

        insert_placeholder(&storage, declared);

        repair_missing_class_definitions_with(storage.clone(), {
            let definition = definition.clone();
            move |_| {
                let definition = definition.clone();
                async move {
                    Ok(DownloadedClass::Cairo {
                        // Return the computed hash, which differs from the declared one.
                        hash: computed,
                        definition,
                    })
                }
            }
        })
        .await
        .unwrap();

        let mut db = storage.connection().unwrap();
        let tx = db.transaction().unwrap();
        // Stored under the declared hash, not the computed one.
        assert_eq!(
            tx.class_definition(declared).unwrap(),
            Some(SerializedOpaqueClassDefinition::from(definition))
        );
        assert_eq!(tx.class_definition(computed).unwrap(), None);
    }

    /// A download failure for one class must not abort the repair of others.
    #[tokio::test]
    async fn partial_failure() {
        let storage = storage();
        let good = ClassHash(pathfinder_crypto::Felt::from_hex_str("0x1111").unwrap());
        let bad = ClassHash(pathfinder_crypto::Felt::from_hex_str("0x2222").unwrap());
        let definition = SerializedCairoDefinition::from_slice(b"good class definition");

        // Insert both in one state update to avoid block number conflicts.
        {
            let mut db = storage.connection().unwrap();
            let tx = db.transaction().unwrap();
            tx.insert_state_update_data(
                BlockNumber::GENESIS,
                &StateUpdateData {
                    declared_cairo_classes: HashSet::from([good, bad]),
                    ..Default::default()
                },
            )
            .unwrap();
            tx.commit().unwrap();
        }

        repair_missing_class_definitions_with(storage.clone(), {
            let definition = definition.clone();
            move |hash| {
                let definition = definition.clone();
                async move {
                    if hash == bad {
                        anyhow::bail!("gateway returned 404");
                    }
                    Ok(DownloadedClass::Cairo { hash, definition })
                }
            }
        })
        .await
        .unwrap();

        let mut db = storage.connection().unwrap();
        let tx = db.transaction().unwrap();
        assert_eq!(
            tx.class_definition(good).unwrap(),
            Some(SerializedOpaqueClassDefinition::from(definition))
        );
        // bad was not repaired — definition IS NULL, so it still appears in
        // the missing list.
        let still_missing = tx.class_hashes_with_missing_definitions().unwrap();
        assert_eq!(still_missing, vec![bad]);
    }
}
