//! Reports how far the precomputed CASM v2 hash table for a given network
//! spans, in terms of block numbers.
//!
//! For the network the DB belongs to (detected from the genesis block hash) it
//! reads the embedded `class_hash -> casm_v2_hash` table, looks up the block at
//! which each class was declared, and reports the highest declaration block
//! encountered together with its Starknet version.
//!
//! USAGE: find_precomputed_casm_v2_span <db_file>

use std::collections::HashMap;

use anyhow::Context;
use pathfinder_common::consts::{
    MAINNET_GENESIS_HASH,
    SEPOLIA_INTEGRATION_GENESIS_HASH,
    SEPOLIA_TESTNET_GENESIS_HASH,
};
use pathfinder_common::StarknetVersion;
use pathfinder_crypto::Felt;
use rusqlite::OptionalExtension;

// The precomputed CASM v2 tables, embedded straight from the casm-hashes crate
// fixtures. Layout: repeated 64-byte records of `class_hash (32) ‖ casm_v2
// (32)`, both big-endian.
const MAINNET_BIN: &[u8] = include_bytes!("../../casm-hashes/fixtures/mainnet-casm-v2-hashes.bin");
const SEPOLIA_TESTNET_BIN: &[u8] =
    include_bytes!("../../casm-hashes/fixtures/testnet-sepolia-casm-v2-hashes.bin");

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    let program = args
        .next()
        .unwrap_or_else(|| "find_precomputed_casm_v2_span".into());
    let Some(database_path) = args.next() else {
        eprintln!("USAGE: {program} <db_file>");
        std::process::exit(1);
    };

    let db = rusqlite::Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .with_context(|| format!("Opening database {database_path}"))?;

    // 1. Detect the network from the genesis (block 0) hash and pick its table.
    let genesis: Vec<u8> = db
        .query_row(
            "SELECT hash FROM block_headers WHERE number = 0",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("Querying genesis block hash")?
        .context("Database has no genesis block (block 0)")?;
    let genesis = Felt::from_be_slice(&genesis).context("Parsing genesis block hash")?;

    let (network, table) = if genesis == MAINNET_GENESIS_HASH.0 {
        ("mainnet", MAINNET_BIN)
    } else if genesis == SEPOLIA_TESTNET_GENESIS_HASH.0 {
        ("sepolia-testnet", SEPOLIA_TESTNET_BIN)
    } else if genesis == SEPOLIA_INTEGRATION_GENESIS_HASH.0 {
        anyhow::bail!("No precomputed CASM v2 table exists for sepolia-integration");
    } else {
        anyhow::bail!("Unknown network: genesis hash {genesis:#x} matches no known network");
    };

    anyhow::ensure!(
        table.len() % 64 == 0,
        "Precomputed table for {network} is malformed: {} bytes is not a multiple of 64",
        table.len()
    );
    let entries = table.len() / 64;
    println!("Detected network     : {network}");
    println!("Table entries        : {entries}");

    // 2. Find the first Starknet 0.14.1 block: the migration boundary. From 0.14.1
    //    the CASM v2 hash is carried in the state diff, so the table (and every
    //    scan below) only concerns blocks strictly before it. Doing this first lets
    //    us bound all subsequent DB work by block number.
    let boundary: u64 = db
        .query_row(
            "SELECT MIN(number) FROM block_headers WHERE version = ?",
            [StarknetVersion::V_0_14_1.as_u32() as i64],
            |row| row.get::<_, Option<i64>>(0),
        )
        .context("Querying first 0.14.1 block")?
        .context("No Starknet 0.14.1 block in the DB — migration not synced, cannot bound scan")?
        as u64;
    println!("First 0.14.1 block   : {boundary}");

    // 3. Bulk-load `class_hash -> declaration block`, bounded to pre-migration
    //    blocks only (everything the table can legitimately contain). Both sides
    //    are normalised through `Felt` so the 32-byte keys match regardless of how
    //    the DB trims leading zero bytes.
    let mut declared_at: HashMap<[u8; 32], u64> = HashMap::new();
    {
        let mut stmt = db.prepare(
            "SELECT hash, block_number FROM class_definitions WHERE block_number IS NOT NULL AND \
             block_number < ?",
        )?;
        let mut rows = stmt.query([boundary as i64])?;
        while let Some(row) = rows.next()? {
            let hash: Vec<u8> = row.get(0)?;
            let block_number: i64 = row.get(1)?;
            let key =
                Felt::from_be_slice(&hash).context("Parsing class hash from class_definitions")?;
            declared_at.insert(*key.as_be_bytes(), block_number as u64);
        }
    }
    println!("Pre-0.14.1 classes   : {}", declared_at.len());
    println!();

    // 4. Walk every entry in the table, resolve its declaration block, and track
    //    the maximum block encountered.
    let mut max_block: Option<u64> = None;
    let mut max_block_class: Option<Felt> = None;
    let mut missing: Vec<Felt> = Vec::new();

    for chunk in table.chunks_exact(64) {
        let class_hash =
            Felt::from_be_slice(&chunk[0..32]).context("Parsing class hash from table")?;

        match declared_at.get(class_hash.as_be_bytes()) {
            Some(&block) => {
                println!("class {class_hash:#x} declared at block {block}");
                if max_block.is_none_or(|m| block > m) {
                    max_block = Some(block);
                    max_block_class = Some(class_hash);
                }
            }
            None => {
                missing.push(class_hash);
                eprintln!(
                    "WARN: class {class_hash:#x} has no declaration block in this DB (not \
                     declared/persisted or reorged out)"
                );
            }
        }
    }

    // 4. Report the highest declaration block and its Starknet version.
    println!();
    println!("--- summary ---");
    println!("network              : {network}");
    println!("table entries        : {entries}");
    println!("missing from DB      : {}", missing.len());
    for class_hash in &missing {
        println!("  missing class      : {class_hash:#x}");
    }

    match (max_block, max_block_class) {
        (Some(block), Some(class_hash)) => {
            // Since revision 0053 the version is a packed u32 column on
            // `block_headers` (the `starknet_versions` table is gone).
            let version: Option<u32> = db
                .query_row(
                    "SELECT version FROM block_headers WHERE number = ?",
                    [block as i64],
                    |row| row.get(0),
                )
                .optional()
                .context("Querying Starknet version for max block")?;

            println!("max declaration block: {block}");
            println!("  declared class     : {class_hash:#x}");
            println!(
                "  starknet version   : {}",
                version
                    .map(|v| StarknetVersion::from_u32(v).to_string())
                    .unwrap_or_else(|| "<unknown>".into())
            );
        }
        _ => println!("No classes from the table were found in this DB."),
    }

    // 5. Upper-end completeness check.
    //
    //    Every Sierra class declared before 0.14.1 must appear in the table
    //    (from 0.14.1 the v2 hash is in the state diff). So any Sierra class
    //    declared in the window (max_block, boundary) is an uncovered
    //    pre-migration class — a hole at the upper end. Because the window is
    //    bounded below 0.14.1 by block number, no version decoding is needed.
    if let Some(max) = max_block {
        let mut stmt = db.prepare(
            "SELECT cd.hash, cd.block_number
             FROM class_definitions cd
             JOIN casm_definitions cs ON cs.hash = cd.hash
             WHERE cd.block_number > ? AND cd.block_number < ?
             ORDER BY cd.block_number",
        )?;
        let mut rows = stmt.query([max as i64, boundary as i64])?;

        let mut holes: Vec<(Felt, u64)> = Vec::new();
        while let Some(row) = rows.next()? {
            let hash: Vec<u8> = row.get(0)?;
            let block_number = row.get::<_, i64>(1)? as u64;
            let class_hash =
                Felt::from_be_slice(&hash).context("Parsing class hash from class_definitions")?;
            holes.push((class_hash, block_number));
        }

        println!();
        println!("--- upper-end completeness ---");
        if holes.is_empty() {
            println!(
                "OK: no Sierra class is declared between block {max} and the 0.14.1 boundary \
                 ({boundary}). The table is complete up to the migration boundary."
            );
        } else {
            println!(
                "missing table entries below first 0.14.1 block: {}",
                holes.len()
            );
            println!();
            println!(
                "INCOMPLETE: {} pre-0.14.1 Sierra class(es) declared after block {max} (below \
                 boundary {boundary}) are NOT in the table:",
                holes.len()
            );
            for (class_hash, block_number) in &holes {
                println!("  {class_hash:#x} at block {block_number}");
            }
        }
    }

    Ok(())
}
