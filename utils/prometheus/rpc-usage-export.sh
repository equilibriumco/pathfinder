#!/usr/bin/env bash
#
# Export a standardized snapshot of Pathfinder's RPC telemetry.
#
# It reads three series Pathfinder emits (see utils/prometheus/rpc-recording-rules.yml):
#   rpc_method_calls_total{method, version, block_target}
#   rpc_method_calls_failed_total{method, version, error_kind}
#   rpc_method_calls_duration_seconds_bucket{le, method, version}
# and writes one JSON document summarizing call volume, how requests target
# blocks, errors by kind, and latency quantiles over a window.
#
# Requires: bash, curl, jq.
#
# See rpc-usage-export.md for more details.

set -euo pipefail

PROM_URL=""
PROVIDER="unknown"
WINDOW="7d"
OUT=""

usage() {
    cat >&2 <<'EOF'
Export a standardized snapshot of Pathfinder's RPC telemetry from Prometheus.

Usage:
  rpc-usage-export.sh --url URL [--provider NAME] [--window 7d] [--out FILE]

  --url URL        Prometheus base URL (required), e.g. http://localhost:9090
  --provider NAME  Label the snapshot with who produced it (default: unknown)
  --window DUR     Look-back window as a PromQL range (default: 7d)
  --out FILE       Write to FILE instead of stdout

Optional auth (pick one, via environment):
  PROM_BEARER_TOKEN=...    sent as `Authorization: Bearer ...`
  PROM_BASIC_AUTH=user:pw  sent as HTTP basic auth
EOF
    exit "${1:-0}"
}

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --url)      PROM_URL="$2"; shift 2 ;;
        --provider) PROVIDER="$2"; shift 2 ;;
        --window)   WINDOW="$2"; shift 2 ;;
        --out)      OUT="$2"; shift 2 ;;
        -h|--help)  usage 0 ;;
        *)          echo "error: unknown argument '$1'" >&2; usage 1 ;;
    esac
done

if [ -z "$PROM_URL" ]; then
    echo "error: --url is required (the Prometheus base URL)" >&2
    usage 1
fi

for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || { echo "error: '$tool' is required but not installed" >&2; exit 1; }
done

# Assemble auth flags from the environment
AUTH=()
if [ -n "${PROM_BEARER_TOKEN:-}" ]; then
    AUTH=(-H "Authorization: Bearer ${PROM_BEARER_TOKEN}")
elif [ -n "${PROM_BASIC_AUTH:-}" ]; then
    AUTH=(-u "${PROM_BASIC_AUTH}")
fi

# Run an instant PromQL query, returning its result vector (.data.result)
prom_query() {
    local query="$1" endpoint response http_code body status
    endpoint="${PROM_URL%/}/api/v1/query"

    # curl exits non-zero only when it can't reach the server at all.
    response=$(curl -sS -g -G ${AUTH[@]+"${AUTH[@]}"} \
        -w $'\n%{http_code}' \
        --data-urlencode "query=${query}" \
        "$endpoint") \
        || { echo "error: could not reach Prometheus at ${PROM_URL}" >&2
             echo "  check the URL and that the server is up and reachable." >&2
             exit 1; }
    http_code=${response##*$'\n'}
    body=${response%$'\n'*}

    # The Prometheus API always answers with JSON, even for a rejected query.
    # A non-JSON body means --url isn't pointing at that API.
    if ! printf '%s' "$body" | jq -e . >/dev/null 2>&1; then
        echo "error: ${endpoint} is not a Prometheus API endpoint (HTTP ${http_code})." >&2
        case "$http_code" in
            404) echo "  pass the Prometheus base URL, e.g. http://host:9090 — not a path or another service." >&2 ;;
            401|403) echo "  authentication is required — set PROM_BEARER_TOKEN or PROM_BASIC_AUTH." >&2 ;;
            *)   echo "  is --url really pointing at Prometheus?" >&2 ;;
        esac
        exit 1
    fi

    status=$(printf '%s' "$body" | jq -r '.status // "error"')
    if [ "$status" != "success" ]; then
        echo "error: Prometheus rejected the query: $(printf '%s' "$body" | jq -r '.error // "unknown error"')" >&2
        echo "  query: ${query}" >&2
        exit 1
    fi
    printf '%s' "$body" | jq '.data.result'
}

# Total calls and error counts over the window; latency quantiles from the histogram
calls=$(prom_query "sum by (method, version, block_target) (increase(rpc_method_calls_total[${WINDOW}]))")
errors=$(prom_query "sum by (method, version, error_kind) (increase(rpc_method_calls_failed_total[${WINDOW}]))")
p50=$(prom_query "histogram_quantile(0.50, sum by (le, method, version) (rate(rpc_method_calls_duration_seconds_bucket[${WINDOW}])))")
p95=$(prom_query "histogram_quantile(0.95, sum by (le, method, version) (rate(rpc_method_calls_duration_seconds_bucket[${WINDOW}])))")
p99=$(prom_query "histogram_quantile(0.99, sum by (le, method, version) (rate(rpc_method_calls_duration_seconds_bucket[${WINDOW}])))")

# A reachable Prometheus with no rpc_* samples usually means it isn't scraping a
# node that emits them: an older Pathfinder, a scrape target that's down, or a
# window longer than retention. Warn rather than hand back a silently empty file.
if [ "$(printf '%s' "$calls" | jq 'length')" = "0" ]; then
    echo "warning: no rpc_method_calls_total data in the last ${WINDOW}." >&2
    echo "  the node may predate these metrics, may not be scraped by this Prometheus," >&2
    echo "  or ${WINDOW} may exceed its retention. the export will be empty." >&2
fi

# Shape the result vectors into the export document
jq -n \
    --arg provider "$PROVIDER" \
    --arg window "$WINDOW" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg prometheus "$PROM_URL" \
    --argjson calls "$calls" \
    --argjson errors "$errors" \
    --argjson p50 "$p50" \
    --argjson p95 "$p95" \
    --argjson p99 "$p99" \
'
# Read a sample value, mapping Prometheus "NaN" (empty bucket) to null
def num: if . == "NaN" then null else tonumber end;
def round2: if . == null then null else (. * 100 | round) / 100 end;
# The histogram is in seconds; report latency in milliseconds for readability
def to_ms: if . == null then null else . * 1000 end;

# Merge the three quantile vectors into one row per method+version. The labels
# are carried in the value, so no key round-trip is needed.
def merge_quantiles($v50; $v95; $v99):
    def fold($vec; $q): reduce $vec[] as $r (.;
        .[$r.metric.method + " " + $r.metric.version] |=
            ((. // {method: $r.metric.method, version: $r.metric.version})
                + {($q): ($r.value[1] | num | to_ms | round2)}));
    {} | fold($v50; "p50") | fold($v95; "p95") | fold($v99; "p99") | [.[]];

{
    provider: $provider,
    window: $window,
    generated_at: $generated_at,
    prometheus: $prometheus,
    metrics: {
        # Total calls per method, summed across how they targeted a block
        calls_by_method: (
            $calls
            | group_by(.metric.method + " " + .metric.version)
            | map({
                method: .[0].metric.method,
                version: .[0].metric.version,
                calls: (map(.value[1] | num) | add | round)
            })
            | sort_by(-.calls)
        ),
        # How requests asked for a block: latest / pending / by_number / by_hash / none / unknown
        block_target_split: (
            $calls
            | map({
                method: .metric.method,
                version: .metric.version,
                block_target: .metric.block_target,
                calls: (.value[1] | num | round)
            })
            | sort_by(-.calls)
        ),
        # Failures by error variant
        errors_by_kind: (
            $errors
            | map({
                method: .metric.method,
                version: .metric.version,
                error_kind: .metric.error_kind,
                errors: (.value[1] | num | round)
            })
            | sort_by(-.errors)
        ),
        # Latency quantiles (milliseconds) per method
        latency_ms: (merge_quantiles($p50; $p95; $p99) | sort_by(-.p99))
    }
}
' > "${OUT:-/dev/stdout}"

if [ -n "$OUT" ]; then
    echo "wrote ${OUT}" >&2
fi
