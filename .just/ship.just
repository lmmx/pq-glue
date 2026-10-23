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

# bump: patch, minor or major, or none to ship the current version as it is (a first
# release). mode: --dry-run makes the release commit's changes and checks them, then puts
# the tree back without committing.
# Ship one commit to crates.io and PyPI at one version (both tagged from it)
ship-rust-and-python bump="patch" mode="":
    #!/usr/bin/env -S echo-comment --shell-flags="-euo pipefail" --color bright-green

    case "{{bump}}" in patch|minor|major|none) ;; *) echo "bump must be patch, minor, major or none, not {{bump}}"; exit 1 ;; esac
    case "{{mode}}" in ""|--dry-run) ;; *) echo "mode must be --dry-run or empty, not {{mode}}"; exit 1 ;; esac

    # 🔍 Checking the branch and the working tree
    branch="$(git rev-parse --abbrev-ref HEAD)"
    if [[ "$branch" != "master" ]]; then
        # ❌ Refusing to run: not on 'master' branch (current: $branch)
        exit 1
    fi
    if [[ -n "$(git status --porcelain)" ]]; then
        # ❌ Refusing to run: the working tree has changes
        git status --short
        exit 1
    fi
    git fetch origin master
    if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/master)" ]]; then
        # ❌ Refusing to run: local master is not level with origin/master
        exit 1
    fi

    py_version="$(uv version --short)"
    rs_version="$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="pq-glue") | .version')"
    if [[ "$py_version" != "$rs_version" ]]; then
        # ❌ pyproject.toml has $py_version but Cargo.toml has $rs_version
        exit 1
    fi
    IFS=. read -r major minor patch <<< "${py_version%%[-+]*}"
    case "{{bump}}" in
        major) version="$((major + 1)).0.0" ;;
        minor) version="$major.$((minor + 1)).0" ;;
        patch) version="$major.$minor.$((patch + 1))" ;;
        none) version="$py_version" ;;
    esac
    # 📦 Shipping pq-glue $version (now $py_version) to crates.io and PyPI

    ## Neither registry may have this version yet
    if curl -sfo /dev/null -A "pq-glue release recipe (https://github.com/lmmx/pq-glue)" "https://crates.io/api/v1/crates/pq-glue/$version"; then
        # ❌ crates.io already has pq-glue $version
        exit 1
    fi
    if curl -sfo /dev/null "https://pypi.org/pypi/pq-glue/$version/json"; then
        # ❌ PyPI already has pq-glue $version
        exit 1
    fi

    # 📈 Setting both versions to $version
    if [[ "$version" != "$py_version" ]]; then
        uv version --frozen "$version"
        sed -i "0,/^version = \".*\"/s//version = \"$version\"/" Cargo.toml
    fi
    cargo update --workspace --quiet

    # 📝 Writing the changelog with release-plz
    release-plz update --allow-dirty
    for got in "$(uv version --short)" "$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="pq-glue") | .version')"; do
        if [[ "$got" != "$version" ]]; then
            # ❌ A version changed to $got, not $version: release-plz moved it. Nothing is committed
            git checkout -- . && git clean -fdq
            exit 1
        fi
    done

    # 🧹 Formatting, then the checks prepush runs
    just precommit || true
    just check clippy docs
    # 📦 Packaging the crate (cargo publish --dry-run)
    cargo publish --dry-run --allow-dirty --quiet

    if [[ "{{mode}}" == "--dry-run" ]]; then
        # 🔎 Dry run: the release commit would hold these changes
        git status --short
        git --no-pager diff --stat
        git checkout -- . && git clean -fdq
        # ✅ Dry run went OK, and the tree is back as it was
        exit 0
    fi

    ## No crab in the message: CI skips a commit with one, so the wheels would not build
    git add --all
    git commit --allow-empty -m "chore(release): ship v$version to crates.io and PyPI"
    # 🚀 Pushing the release commit
    git push --no-verify origin master

    # 🦀 Publishing the crate: release-plz tags pq-glue-v$version and makes the GitHub release
    just publish-rust

    # 🐍 Tagging py-$version: CI builds the wheels from it and publishes them to PyPI
    git tag -a "py-$version" -m "Python Release $version"
    git push origin "py-$version"
    # ✅ Shipped $version: CI on the py-$version tag publishes the wheels to PyPI

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
