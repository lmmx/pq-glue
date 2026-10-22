#!/usr/bin/env python3
"""Check the documentation site's sources and its built output.

    uv run --no-project -p 3.13 python docs/scripts/check_site.py [--site DIR]

- Every page under docs/site/ is in the nav of zensical.toml, and every nav entry exists.
- Every internal link, image and stylesheet in the built site (default: site/) resolves to
  a file of the build, with its #anchor present, as served under the base path of
  site_url (/pq-glue/ on GitHub Pages).
- Every nav page has a front-matter description, and is listed once in the llmstxt sections
  of zensical.toml with that same description; the built site has llms.txt, llms-full.txt
  and the page's Markdown copy, and no Markdown copy of any other page.
- API coverage: every public module of python/pq_glue/ has a reference page with its `:::`
  directive, and every name in each public module's `__all__` (read statically with ast)
  is documented: the built reference has an anchor for it, under that module or, for a
  re-export, under the module it comes from.

Exits with 1 and lists every problem found.
"""

import argparse
import ast
import re
import sys
import tomllib
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse


ROOT = Path(__file__).resolve().parents[2]


def nav_pages(nav):
    for entry in nav:
        if isinstance(entry, str):
            yield entry
        elif isinstance(entry, dict):
            for value in entry.values():
                yield from nav_pages([value] if isinstance(value, str) else value)


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links, self.ids = [], set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        if tag == "a" and attrs.get("name"):
            self.ids.add(attrs["name"])
        for key in ("href", "src"):
            value = attrs.get(key)
            if value and not (
                tag == "link" and attrs.get("rel") in ("preconnect", "dns-prefetch")
            ):
                self.links.append(value)


def check_nav(config, problems):
    docs = ROOT / config["project"]["docs_dir"]
    pages = {p.relative_to(docs).as_posix() for p in docs.rglob("*.md")}
    listed = set(nav_pages(config["project"]["nav"]))
    for page in sorted(pages - listed):
        problems.append(f"not in the nav: {page}")
    for page in sorted(listed - pages):
        problems.append(f"in the nav but missing: {page}")


def check_links(config, site, problems):
    base = urlparse(config["project"]["site_url"]).path  # "/pq-glue/"
    parsed = {}
    for html in site.rglob("*.html"):
        p = Links()
        p.feed(html.read_text(errors="replace"))
        parsed[html] = p

    def target(url_path):
        if not url_path.startswith(base):
            return None
        rel = unquote(url_path[len(base) :])
        path = site / rel
        if url_path.endswith("/") or path.is_dir():
            path = path / "index.html"
        return path

    checked = 0
    for html, p in sorted(parsed.items()):
        page_url = base + html.relative_to(site).as_posix()
        if page_url.endswith("index.html"):
            page_url = page_url[: -len("index.html")]
        for link in p.links:
            url = urlparse(urljoin("https://host" + page_url, link))
            if (
                urlparse(link).scheme
                and urlparse(link).netloc
                and not link.startswith(config["project"]["site_url"])
            ):
                continue  # external
            if link.startswith(("mailto:", "javascript:", "data:")):
                continue
            path = target(url.path)
            checked += 1
            where = html.relative_to(site).as_posix()
            if path is None:
                problems.append(f"{where}: {link} leaves the base path {base}")
            elif not path.exists():
                problems.append(f"{where}: {link} -> missing {path.relative_to(site)}")
            elif url.fragment and path.suffix == ".html":
                ids = parsed[path].ids if path in parsed else set()
                if url.fragment not in ids:
                    problems.append(f"{where}: {link} -> no #{url.fragment}")
    return checked


def check_llms(config, site, problems):
    docs = ROOT / config["project"]["docs_dir"]
    sections = config["project"]["plugins"]["llmstxt"]["sections"]
    listed = [
        entry
        for entries in sections.values()
        for item in entries
        for entry in item.items()
    ]
    paths = [path for path, _ in listed]
    for page in sorted({p for p in paths if paths.count(p) > 1}):
        problems.append(f"listed more than once in the llmstxt sections: {page}")
    for page in nav_pages(config["project"]["nav"]):
        text = (docs / page).read_text() if (docs / page).exists() else ""
        m = re.match(r'---\n(?:.*\n)*?description: "(.*)"\n(?:.*\n)*?---\n', text)
        if not m:
            problems.append(f"no front-matter description: {page}")
        if page not in paths:
            problems.append(f"not in the llmstxt sections of zensical.toml: {page}")
        elif m and dict(listed)[page] != m.group(1):
            problems.append(
                f"llmstxt description differs from the front matter: {page}"
            )
    if (site / "index.html").exists():
        for name in ("llms.txt", "llms-full.txt"):
            if not (site / name).exists():
                problems.append(f"no {name} in the built site")
        for page in paths:
            md = md_copy(site, page)
            if not md.exists():
                problems.append(f"no Markdown copy of {page} at {md.relative_to(site)}")
        expected = {md_copy(site, page) for page in paths}
        for md in sorted(set(site.rglob("*.md")) - expected):
            page = md.relative_to(site).as_posix()
            problems.append(
                f"Markdown copy of a page not in the llmstxt sections: {page}"
            )


def md_copy(site, page):
    """Where the llmstxt plugin writes the Markdown copy of a page."""
    if page == "index.md" or page.endswith("/index.md"):
        return site / page
    return site / page.removesuffix(".md") / "index.md"


def public_names(path):
    """The module's literal `__all__`, else the names it defines without a leading `_`."""
    body = ast.parse(path.read_text()).body
    for node in body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            return [elt.value for elt in node.value.elts], True
    names = []
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            names += [t.id for t in node.targets if isinstance(t, ast.Name)]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.append(node.target.id)
    return [n for n in names if not n.startswith("_")], False


def check_api(config, site, problems):
    docs = ROOT / config["project"]["docs_dir"]
    src = ROOT / "python" / "pq_glue"
    public = ["pq_glue"] + sorted(
        f"pq_glue.{p.stem}" for p in src.glob("*.py") if not p.stem.startswith("_")
    )
    directives = {}
    for page in docs.rglob("*.md"):
        for ident in re.findall(r"^::: *([\w.]+)", page.read_text(), re.M):
            directives.setdefault(ident, page.relative_to(docs).as_posix())
    for mod in public:
        if mod not in directives:
            problems.append(f"public module without a `::: {mod}` reference page")
    ids = set()
    for html in (site / "reference").rglob("*.html"):
        p = Links()
        p.feed(html.read_text(errors="replace"))
        ids |= p.ids
    for mod in public:
        path = src / "__init__.py" if mod == "pq_glue" else src / f"{mod.split('.')[1]}.py"
        names, declared = public_names(path)
        if not declared:
            print(
                f"note: {mod} has no __all__; checking its {len(names)} public names",
                file=sys.stderr,
            )
        for name in names:
            # a re-export is documented under any public module
            if f"{mod}.{name}" not in ids and not any(
                f"{m}.{name}" in ids for m in public
            ):
                problems.append(
                    f"{mod}.{name} is in __all__ but not in the API reference"
                )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", default=str(ROOT / "site"))
    args = parser.parse_args()
    config = tomllib.loads((ROOT / "zensical.toml").read_text())
    problems = []
    check_nav(config, problems)
    site = Path(args.site)
    if (site / "index.html").exists():
        checked = check_links(config, site, problems)
        print(f"{checked} internal links checked", file=sys.stderr)
    else:
        problems.append(f"no built site at {site} (zensical build)")
    check_llms(config, site, problems)
    if (site / "index.html").exists():
        check_api(config, site, problems)
    for problem in problems:
        print(problem)
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
