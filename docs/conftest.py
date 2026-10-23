"""Run the code examples of the documentation site as tests.

    uv run pytest docs

Every page under docs/site/ with ```python (or ```py) or ```pycon fences is one test.
The page's blocks run in order in one namespace, like a session, in a fresh temporary
working directory, so examples can build on each other and write relative paths. A
``pycon`` block is a doctest: its output must match. A fence whose info string contains
``notest`` (```python notest) is skipped. A fence inside an HTML comment runs but is not
shown on the page, for setup such as creating the files an example reads.
"""

from __future__ import annotations

import doctest
import os
import re
import textwrap
import traceback
from pathlib import Path

import pytest


SITE = Path(__file__).parent / "site"
FENCE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<ticks>`{3,})(?P<info>[^\n`]*)\n(?P<body>.*?)^(?P=indent)(?P=ticks)[ \t]*$",
    re.M | re.S,
)
LANGS = {"python": "python", "py": "python", "python3": "python", "pycon": "pycon"}
FLAGS = doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE


def blocks(text: str):
    """Yield each testable fence in `text` as (language, first body line, body)."""
    for m in FENCE.finditer(text):
        info = m["info"].strip().strip("{}").split()
        lang = LANGS.get(info[0].lstrip(".")) if info else None
        if lang and "notest" not in info:
            line = text.count("\n", 0, m.start()) + 2  # first line of the body
            yield lang, line, textwrap.dedent(m["body"])


def pytest_collect_file(file_path: Path, parent):
    """Collect a page under docs/site/ that has testable examples."""
    if file_path.suffix == ".md" and file_path.is_relative_to(SITE):
        found = list(blocks(file_path.read_text()))
        if found:
            return MarkdownPage.from_parent(parent, path=file_path, found=found)
    return None


class MarkdownPage(pytest.File):
    """A documentation page, collected as one item of its examples."""

    def __init__(self, *, found, **kw):
        super().__init__(**kw)
        self.found = found

    def collect(self):
        """Yield the page's examples as one item."""
        yield MarkdownExamples.from_parent(self, name="examples", found=self.found)


class ExampleError(Exception):
    """An example failed: the message holds its location and output."""


class MarkdownExamples(pytest.Item):
    """A page's examples, run in order in one namespace, in a temporary directory."""

    def __init__(self, *, found, **kw):
        super().__init__(**kw)
        self.found = found

    def runtest(self):
        """Run each example: Python blocks with exec, pycon blocks as doctests."""
        tmp = self.config._tmp_path_factory.mktemp("docs-example")  # noqa: SLF001
        globs = {"__name__": "__docs__"}
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            for lang, line, body in self.found:
                where = f"{self.path.relative_to(SITE.parent.parent)}:{line}"
                if lang == "python":
                    try:
                        # dont_inherit: without it the page's code would run under this
                        # module's `from __future__ import annotations`, and a dataclass
                        # in it fails (its string annotations need a module to resolve in).
                        code = compile(
                            "\n" * (line - 1) + body,
                            str(self.path),
                            "exec",
                            dont_inherit=True,
                        )
                        exec(code, globs)  # noqa: S102
                    except Exception:
                        raise ExampleError(
                            f"{where}\n{traceback.format_exc()}"
                        ) from None
                else:
                    test = doctest.DocTestParser().get_doctest(
                        body, globs, where, str(self.path), line - 1
                    )
                    out: list[str] = []
                    runner = doctest.DocTestRunner(optionflags=FLAGS)
                    runner.run(test, out=out.append, clear_globs=False)
                    globs.update(test.globs)  # DocTest copies globs; keep its names
                    if runner.failures:
                        raise ExampleError("".join(out))
        finally:
            os.chdir(cwd)

    def repr_failure(self, excinfo):
        """Report a failed example by its location and output, without pytest's traceback."""
        if isinstance(excinfo.value, ExampleError):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self):
        """Name the item after its page."""
        return self.path, 0, f"examples in {self.path.name}"
