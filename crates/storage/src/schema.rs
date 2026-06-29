mod base;

mod revision_0041;
mod revision_0042;
mod revision_0043;
mod revision_0044;
mod revision_0045;
mod revision_0046;
mod revision_0047;
mod revision_0048;
mod revision_0049;
mod revision_0050;
mod revision_0051;
mod revision_0052;
mod revision_0053;
mod revision_0054;
mod revision_0055;
mod revision_0056;
mod revision_0057;
mod revision_0058;
mod revision_0059;
mod revision_0060;
mod revision_0061;
mod revision_0062;
mod revision_0063;
mod revision_0064;
mod revision_0065;
mod revision_0066;
mod revision_0067;
mod revision_0068;
mod revision_0069;
mod revision_0070;
mod revision_0071;
mod revision_0072;
pub(crate) mod revision_0073;
mod revision_0074;
mod revision_0075;
mod revision_0076;
mod revision_0077;
mod revision_0078;
mod revision_0079;

pub(crate) use base::base_schema;

type MigrationFn = fn(&rusqlite::Transaction<'_>, &crate::RocksDBInner) -> anyhow::Result<()>;

/// The full list of pathfinder migrations.
pub fn migrations() -> &'static [MigrationFn] {
    MIGRATIONS
}

/// The number of schema revisions replaced by the [base
/// schema](base::base_schema).
///
/// Note that 40 was a no-op as we wanted to disallow versions <= 39.
pub(crate) const BASE_SCHEMA_REVISION: usize = 40;

const MIGRATIONS: &[MigrationFn] = &[
    revision_0041::migrate,
    revision_0042::migrate,
    revision_0043::migrate,
    revision_0044::migrate,
    revision_0045::migrate,
    revision_0046::migrate,
    revision_0047::migrate,
    revision_0048::migrate,
    revision_0049::migrate,
    revision_0050::migrate,
    revision_0051::migrate,
    revision_0052::migrate,
    revision_0053::migrate,
    revision_0054::migrate,
    revision_0055::migrate,
    revision_0056::migrate,
    revision_0057::migrate,
    revision_0058::migrate,
    revision_0059::migrate,
    revision_0060::migrate,
    revision_0061::migrate,
    revision_0062::migrate,
    revision_0063::migrate,
    revision_0064::migrate,
    revision_0065::migrate,
    revision_0066::migrate,
    revision_0067::migrate,
    revision_0068::migrate,
    revision_0069::migrate,
    revision_0070::migrate,
    revision_0071::migrate,
    revision_0072::migrate,
    revision_0073::migrate,
    revision_0074::migrate,
    revision_0075::migrate,
    revision_0076::migrate,
    revision_0077::migrate,
    revision_0078::migrate,
    revision_0079::migrate,
];

// The target version is the number of null migrations which have been replaced
// by the base schema + the new migrations built on top of that.
pub(crate) const LATEST_SCHEMA_REVISION: usize = BASE_SCHEMA_REVISION + MIGRATIONS.len();
