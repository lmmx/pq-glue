# Release a new version, pass --help for options to `uv version --bump`
release bump_level="patch":
    #!/usr/bin/env -S echo-comment --shell-flags="-e" --color bright-green

    ## Exit early if help was requested
    if [[ "{{bump_level}}" == "--help" ]]; then
        uv version --help
        exit 0
    fi

    # 📈 Bump the version in pyproject.toml (patch/minor/major: {{bump_level}})
    uv version --frozen --bump {{bump_level}}

    # 🦀 Give the crate the same version, and let cargo synchronise Cargo.lock
    new_version=$(uv version --short)
    sed -i "0,/^version = \".*\"/s//version = \"$new_version\"/" Cargo.toml
    cargo update --workspace --quiet

    # 📦 Stage all changes (including the version bump)
    git add --all

    # ✅ Create the release commit
    git commit -m  "chore(release): bump 🐍 -> v$new_version"

    # 🏷️ Create the git tag for this release
    git tag -a "py-$new_version" -m "Python Release $new_version"

    branch_name=$(git rev-parse --abbrev-ref HEAD);
    # 🚀 Push the release commit to $branch_name
    git push origin $branch_name

    # 🚀 Push the commit tag to the remote
    git push origin "py-$new_version"

# --------------------------------------------------------------------------------------------------

# Rust release workflow using release-plz
ship-rust version="auto":
    #!/usr/bin/env -S echo-comment --shell-flags="-euo pipefail" --color="\\033[38;5;202m"

    # 🔍 Refuse to run if not on master branch or not up to date with origin/master
    branch="$(git rev-parse --abbrev-ref HEAD)"
    if [[ "$branch" != "master" ]]; then
        # ❌ Refusing to run: not on 'master' branch (current: $branch)
        exit 1
    fi

    # 🔍 Fetch master branch
    git fetch origin master
    local_rev="$(git rev-parse HEAD)"
    remote_rev="$(git rev-parse origin/master)"
    # 🔍 Local: $local_rev\n🔍 Remote: $remote_rev
    if [[ "$local_rev" != "$remote_rev" ]]; then
        # ❌ Refusing to run: local master branch is not up to date with origin/master
        # Local HEAD:  $local_rev
        # Origin HEAD: $remote_rev
        # Please pull/rebase to update.
        exit 1
    fi

    # 🔍 Dry-run release...
    just publish-rust --dry-run
    # ✅ Dry-run went OK, proceeding to real release

    if [[ "{{version}}" != "auto" ]]; then
        if [[ -n "$(git status --porcelain)" ]]; then
            # ❌ Working directory must be clean for manual version bump
            git status --short
            exit 1
        fi

        # 🦀 Explicit version bump with release-plz
        release-plz set-version pq-glue@{{version}}

        # 🦀 Update Cargo.toml versions and changelogs
        release-plz update --allow-dirty
    else
        # 🦀 Auto update Cargo.toml versions and changelogs
        release-plz update
    fi

    # Run pre-precommit fixers to avoid the linter halting our release!
    just precommit || true
    # Let cargo synchronise Cargo.lock
    cargo check --quiet
    git add .
    git commit -m "chore(release): 🦀 Upgrades"

    # 🦀 Run prepush before releasing the crate
    just check clippy docs
    # 🚀 Push the version bump commit
    git push --no-verify

    # 📦 Create releases and tags
    just publish-rust

publish-rust mode="":
    #!/usr/bin/env -S bash -euo pipefail
    git_token=$(gh auth token 2>/dev/null) || git_token=$PUBLISH_GITHUB_TOKEN

    ## 🦀 Let release-plz handle the crate's tag (pq-glue-v0.1.1, etc.)
    release-plz release --backend github --git-token $git_token {{mode}}

# For when release-plz isn't working
ship-rust-manual:
    #!/usr/bin/env -S echo-comment --shell-flags="-euo pipefail" --color="\\033[38;5;202m"

    # Bump the patch version
    cargo set-version --bump patch

    # Commit and tag
    git add .
    git commit -m "chore(release): 🦀 Upgrades"

    git tag pq-glue-v$(cargo metadata --no-deps --format-version 1 \
      | jq -r '.packages[] | select(.name=="pq-glue") | .version')

    # Push to remote
    git push
    git push --tags

    cargo publish
