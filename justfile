default:
    just --summary --unsorted

test $RUST_BACKTRACE="1" *args="": build-pathfinder-release
    cargo nextest run --no-fail-fast --all-targets --features p2p --workspace --locked \
    -E 'not (test(/^p2p_network::sync::sync_handlers::tests::prop/) | test(/^test::consensus_[34]_nodes/))' \
    {{args}}

test-all-features $RUST_BACKTRACE="1" *args="": build-pathfinder-release
    cargo nextest run --no-fail-fast --all-targets --all-features --workspace --locked \
    -E 'not (test(/^p2p_network::sync::sync_handlers::tests::prop/) | test(/^test::consensus_[34]_nodes/))' \
    {{args}}

test-consensus $RUST_BACKTRACE="1" *args="": build-pathfinder build-feeder-gateway
    PATHFINDER_TEST_ENABLE_MARKER_FILES=1 cargo nextest run --test consensus -p pathfinder --retries 2 -j 1 --features p2p,consensus-integration-tests --locked \
    {{args}}

proptest-sync-handlers $RUST_BACKTRACE="1" *args="":
    cargo nextest run --no-fail-fast --retries 2 --all-targets --features p2p --workspace --locked \
    -E 'test(/^p2p_network::sync::sync_handlers::tests::prop/)' \
    {{args}}

build:
    cargo build --workspace --all-targets

build-all-features:
    cargo build --workspace --all-targets --all-features

# This target is used in `integration_testing_cli` test.
build-pathfinder-release:
    cargo build --release -p pathfinder --bin pathfinder -F p2p,consensus-integration-tests

build-pathfinder:
    cargo build -p pathfinder --bin pathfinder -F p2p,consensus-integration-tests

build-feeder-gateway:
    cargo build -p feeder-gateway -F small_aggregate_filters --bin feeder-gateway

check:
    cargo check --workspace --all-targets

check-all-features:
    cargo check --workspace --all-targets --all-features

fmt:
    cargo +nightly fmt --all

clippy *args="":
    cargo clippy --workspace --all-targets --features p2p --locked {{args}} -- -D warnings -D rust_2018_idioms

clippy-all-features *args="":
    cargo clippy --workspace --all-targets --all-features --locked {{args}} -- -D warnings -D rust_2018_idioms

dep-sort:
    cargo sort --check --workspace

doc:
    cargo doc --no-deps --document-private-items

release version:
    scripts/release.sh {{version}}

alias b := build 
alias t := test 
alias c := check 
alias f := fmt 
alias r := release
