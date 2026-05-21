//! The purpose if this migration is to repair any database instances that were
//! affected by a combination of a bug introduced in
//! [revision 71](super::revision_0071) and a reorg described in the issue that
//! can be found [here](https://github.com/equilibriumco/pathfinder/issues/2920).
//!
//! The revision also contains a suite of
//! [regression checks](reorg_regression_checks) that will be used to verify
//! that the mentioned bug has been fixed and that the rest of the functionality
//! affected by the migration is still correct.

use anyhow::Context;

use crate::prelude::*;

pub(crate) fn migrate(
    tx: &rusqlite::Transaction<'_>,
    _rocksdb: &crate::RocksDBInner,
) -> anyhow::Result<()> {
    tracing::info!("Setting block numbers for known classes affected by a migration bug to NULL");

    let known_affected_class_hashes = [
        pathfinder_common::class_hash!(
            "0x045407B5C7D5823F2DB10470C48F1B18C40F6A9543399E650C9A38A0F6251FFC"
        ),
        pathfinder_common::class_hash!(
            "0x07E4359644723B854E30F650297EDA37FACF490C93AC119FBFF079CA2667C461"
        ),
    ];

    for class_hash in known_affected_class_hashes {
        let class_found_in_redeclared: bool = tx
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM redeclared_classes WHERE class_hash = ?)",
                params![&class_hash],
                |row| row.get(0),
            )
            .context("Checking if class is in redeclared_classes")?;

        if !class_found_in_redeclared {
            tx.execute(
                "UPDATE class_definitions SET block_number = NULL WHERE hash = ?",
                params![&class_hash],
            )
            .context("Setting block number to NULL")?;
        }
    }

    Ok(())
}

pub mod reorg_regression_checks {
    use pathfinder_common::ClassHash;

    use super::*;

    /// Contract updates for the purged blocks should be deleted.
    pub fn contract_updates_deleted(
        tx: &mut Transaction<'_>,
        reorg_tail: pathfinder_common::BlockNumber,
    ) -> bool {
        let num_contract_updates_after_reorg_tail: u64 = tx
            .inner()
            .query_row(
                "SELECT COUNT(*) FROM contract_updates WHERE block_number >= :reorg_tail",
                named_params! { ":reorg_tail": &reorg_tail },
                |row| row.get(0),
            )
            .unwrap();

        num_contract_updates_after_reorg_tail == 0
    }

    /// Block numbers inside the `class_definitions` table should be set to NULL
    /// for all classes declared in the purged blocks.
    pub fn class_definition_removed(tx: &mut Transaction<'_>, class_hash: ClassHash) -> bool {
        tx.class_definition_with_block_number(class_hash)
            .unwrap()
            .expect("class definition preset")
            .0
            .is_none()
    }
}
