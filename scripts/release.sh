#!/bin/bash
set -e

VERSION=$1

# Check if tag already exists
if git rev-parse "v${VERSION}" >/dev/null 2>&1; then
    echo "Error: Tag v${VERSION} already exists!"
    echo "Please remove the tag running 'git tag -d v${VERSION}' and try again."
    exit 1
fi

# Check if branch already exists
if git show-ref --verify --quiet "refs/heads/release/v${VERSION}" || \
   git show-ref --verify --quiet "refs/remotes/origin/release/v${VERSION}"; then
    echo "Error: Branch release/v${VERSION} already exists locally or remotely!"
    echo "Please remove the branch running 'git branch -D release/v${VERSION}' and try again."
    exit 1
fi

# Verify version format
if ! echo "$VERSION" | grep -E "^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9]+(\.[0-9]+)?)?$" > /dev/null; then
    echo "Version must be in format X.Y.Z or X.Y.Z-SUFFIX[.N] (e.g., 1.2.3 or 1.2.3-beta.0)"
    exit 1
fi

# Cross-platform sed command (macOS and Linux)
do_sed() {
    local file=$1
    local pattern=$2
    if [[ "$(uname)" == "Darwin" ]]; then
        sed -i '.bak' -E "$pattern" "$file"
        rm "${file}.bak"
    else
        sed -i -E "$pattern" "$file"
    fi
}

# Update CHANGELOG.md - replace the Unreleased heading with version and date.
# Accepts either "## Unreleased" or "## [Unreleased]" (as used by Keep a Changelog).
CURRENT_DATE=$(date +%Y-%m-%d)
UNRELEASED_PATTERN='^## \[?Unreleased\]?[[:space:]]*$'
if ! grep -qE "$UNRELEASED_PATTERN" CHANGELOG.md; then
    echo "Error: Could not find an '## Unreleased' or '## [Unreleased]' heading in CHANGELOG.md"
    echo "Please add an unreleased section before running this script."
    exit 1
fi
do_sed "CHANGELOG.md" "s/${UNRELEASED_PATTERN}/## [${VERSION}] - ${CURRENT_DATE}/"

# Update workspace version
do_sed "Cargo.toml" "s/^version = \".*\"/version = \"${VERSION}\"/"

# List of crates that require explicit version dependencies for publishing
CRATES=("common" "crypto" "serde" "class-hash" "consensus")

# Update versions in crate Cargo.toml files
for crate in "${CRATES[@]}"; do
    file="crates/${crate}/Cargo.toml"
    echo "Updating dependencies for $file..."
    do_sed "$file" "s/pathfinder-common = \{ version = \"[^\"]*\"/pathfinder-common = { version = \"${VERSION}\"/"
    do_sed "$file" "s/pathfinder-crypto = \{ version = \"[^\"]*\"/pathfinder-crypto = { version = \"${VERSION}\"/"
    do_sed "$file" "s/pathfinder-serde = \{ version = \"[^\"]*\"/pathfinder-serde = { version = \"${VERSION}\"/"
done

# Update Cargo.lock and verify everything still builds
cargo check --workspace --all-targets
cargo check --workspace --all-targets --all-features
# The `load-test` crate is excluded from the workspace but has a dependency on `pathfinder-common`.
pushd crates/load-test
cargo check
popd

# Create and checkout new release branch
git checkout -b release/v${VERSION}

# Create git commit
git add Cargo.toml Cargo.lock **/Cargo.toml **/Cargo.lock CHANGELOG.md
git commit -m "chore: bump version to ${VERSION}"

# Quick recap of what was done
echo -e "\nChanges made:"
echo "- Updated workspace version to ${VERSION}"
echo "- Updated CHANGELOG.md with version ${VERSION} and date ${CURRENT_DATE}"
echo "- Updated dependency versions in public crates:"
for crate in "${CRATES[@]}"; do
    echo "  - crates/${crate}/Cargo.toml"
done

# Confirmation before pushing
echo -e "\nPush these changes to \`release/v${VERSION}\`? (Y/n)"
read -r answer
if [[ "$answer" == "n" ]] || [[ "$answer" == "N" ]]; then
    echo "Aborting push. Changes are committed locally."
    exit 1
fi

# Push changes
git push --set-upstream origin release/v${VERSION}

# Try to create PR automatically if gh CLI is available
if command -v gh &> /dev/null; then
    echo -e "\nCreating PR automatically with gh CLI..."
    if PR_URL=$(gh pr create --title "chore: bump version to ${VERSION}" --body "Automated release PR for version ${VERSION}" --base main --head release/v${VERSION} 2>/dev/null); then
        echo "PR created: $PR_URL"
        echo -e "\nPlease review and merge the PR, then come back here and press Enter to continue..."
    else
        echo "Failed to create PR with gh CLI. Falling back to manual instructions..."
        echo -e "\n"
        echo "=========================================="
        echo "Next steps:"
        echo "1. Create a PR for branch 'release/v${VERSION}'"
        echo "2. Review and merge the PR to 'main'"
        echo "3. Come back here and press Enter to continue"
        echo "=========================================="
        echo -e "\nPress Enter once the PR has been merged to continue..."
    fi
else
    echo -e "\n"
    echo "=========================================="
    echo "Next steps:"
    echo "1. Create a PR for branch 'release/v${VERSION}'"
    echo "2. Review and merge the PR to 'main'"
    echo "3. Come back here and press Enter to continue"
    echo "=========================================="
    echo -e "\nPress Enter once the PR has been merged to continue..."
fi
read -r

# Switch to main and pull latest
git checkout main
git pull origin main

# Confirmation before creating tag
echo -e "\nReady to create and push tag 'v${VERSION}'"
echo "This will trigger the Release and Docker workflows."
echo -e "\nCreate and push tag 'v${VERSION}'? (Y/n)"
read -r answer
if [[ "$answer" == "n" ]] || [[ "$answer" == "N" ]]; then
    echo "Aborting tag creation."
    exit 1
fi

# Create and push tag
echo "Creating and pushing tag v${VERSION}..."
git tag -a v${VERSION} -m "Pathfinder v${VERSION}"
git push origin v${VERSION}

# Done
echo -e "\n✅ Tag 'v${VERSION}' has been pushed!"
echo "The Release and Docker workflows should now be triggered."
echo "You can monitor their progress in the GitHub Actions tab."
