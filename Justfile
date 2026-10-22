import ".just/commit.just"
import ".just/docs.just"
import ".just/python.just"
import ".just/quality.just"
import ".just/rust.just"
import ".just/ship.just"

default: clippy

ci_opt := if env("PRE_COMMIT_HOME", "") != "" { "-ci" } else { "" }

precommit:
    just pc{{ci_opt}}

pc:     fmt code-quality lint
pc-fix: fmt code-quality-fix
pc-ci:      code-quality

prepush: check clippy docs py

# (Not running ty in lint recipe)
lint: ruff-check # lint-action

fmt:     ruff-fmt code-quality-fix

full:    pc prepush build test py
full-ci: pc-ci prepush         py

# usage:
#   just e                -> open Justfile normally
#   just e foo            -> search for "foo" and open Justfile at that line
#   just e @bar           -> search for "^bar" (recipe name) and open Justfile at that line
#
e target="":
    #!/usr/bin/env -S echo-comment --color bold-red
    if [[ "{{target}}" == "" ]]; then
      $EDITOR Justfile
    else
      pat="{{target}}"
      if [[ "$pat" == @* ]]; then
        pat="^${pat:1}"   # strip @ and prefix with ^
      fi
      line=$(rg -n "$pat" Justfile .just/*.just | head -n1 | cut -d: -f1,2)
      if [[ -n "$line" ]]; then
        $EDITOR +${line#*:} ${line%%:*}
      else
        # No match for: $pat
        exit 1
      fi
    fi

lint-action:
    actionlint .github/workflows/*.yml
