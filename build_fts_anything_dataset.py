# -*- coding: utf-8 -*-

"""
This module can scan a read-the-docs style documentation project, and build dataset for
`Full-text-search-anything Alfred Workflow <https://github.com/MacHu-GWU/afwf_fts_anything-project>`_.

So user can use `Alfred <https://www.alfredapp.com/>`_ to search and jump to the
corresponding https://${project_name}.readthedocs.io/.../index.html page.
"""

import typing as T
import json
import shutil
from pathlib import Path


header1_rst = "=" * 40
header1_md = "# "


DEFAULT_DELIMITERS = list(",;.")


def tokenize(s: str, delimiters: T.Optional[T.List[str]] = None) -> T.List[str]:
    """
    Tokenize a string into a list of words.
    """
    if delimiters is None:
        delimiters = DEFAULT_DELIMITERS
    for delimiter in delimiters:
        s = s.replace(delimiter, " ")
    words = [word for word in s.split() if word]
    return words


class FailedToExtractTitleAndKeywords(ValueError):
    pass

class UnknownIndexFileType(TypeError):
    pass


def extract_title_and_keywords_from_rst(
    path_index_rst: Path,
) -> T.Tuple[str, T.List[str]]:
    """
    获取 ``index.rst`` 文档的 title 和 keywords. 文档内容必须是以 "======" 作为 header1
    的文档, 然后 header1 下面会有一个 ``Keywords: word1, word2, ...`` 的行. 其中
    header1 的文本就是 Title. 我们可以允许没有 keywords, 但是一定要有 title. 否则就会抛出
    异常.
    """
    lines = path_index_rst.read_text().splitlines()
    title = None
    for ind, line in enumerate(lines):
        if line.startswith(header1_rst):
            title = lines[ind - 1]
        line = line.lower()
        if line.startswith("keywords: "):
            keywords = tokenize(line.lstrip("keywords: "))
            if title is not None:
                return title, keywords
    if title is not None:
        return (title, [])
    else:
        raise FailedToExtractTitleAndKeywords


def extract_title_and_keywords_from_ipynb(
    path_index_ipynb: Path,
) -> T.Tuple[str, T.List[str]]:
    """
    获取 ``index.ipynb`` 文档的 title 和 keywords. 文档内容必须是以 "# ${title}" 作为 header1
    的文档, 然后 header1 下面会有一个 ``Keywords: word1, word2, ...`` 的行. 其中
    header1 的文本就是 Title. 我们可以允许没有 keywords, 但是一定要有 title. 否则就会抛出
    异常.
    """
    data = json.loads(path_index_ipynb.read_text())
    for cell in data["cells"]:
        if cell["cell_type"] == "markdown":
            if len(cell["source"]):
                lines = cell["source"]
                title = None
                for ind, line in enumerate(lines):
                    if line.startswith(header1_md):
                        title = line[2:].strip()
                    line = line.lower()
                    if line.startswith("keywords: "):
                        keywords = tokenize(line.lstrip("keywords: "))
                        if title is not None:
                            return title, keywords
                if title is not None:
                    return (title, [])
                else:
                    raise FailedToExtractTitleAndKeywords


def extract_title_and_keywords(
    path_index: Path,
) -> T.Tuple[str, T.List[str]]:  # pragma: no cover
    if path_index.suffix == ".rst":
        return extract_title_and_keywords_from_rst(path_index)
    elif path_index.suffix == ".ipynb":
        return extract_title_and_keywords_from_ipynb(path_index)
    else:
        raise UnknownIndexFileType(f"Unknown file type: {path_index}")


def get_url(
    domain: str,
    dir_source: Path,
    path_index_rst: Path,
):
    """
    Example:

        >>> domain = "https://my_dataset.readthedocs.io/"
        >>> dir_source = Path("/Path/to/source")
        >>> path_index_rst = Path("/Path/to/source/chapter1/section1/index.rst")
        >>> get_url(domain, dir_source, path_index_rst)
        "https://my_dataset.readthedocs.io/chapter1/section1/index.html"
    """
    if domain.endswith("/"):
        domain = domain[:-1]
    relpath = path_index_rst.parent.joinpath(f"{path_index_rst.stem}.html").relative_to(
        dir_source
    )
    url = f"{domain}/{relpath}"
    return url


_dir_here = Path(__file__).absolute().parent
_dir_source = _dir_here.joinpath("docs", "source")
_dir_home = Path.home()
_dir_afwf_fts_anything = _dir_home.joinpath(".alfred-afwf", "afwf_fts_anything")


def build_fts_anything_dataset(
    domain: str,
    dataset_name: str,
    dir_source: Path = _dir_source,
    dir_afwf_fts_anything: Path = _dir_afwf_fts_anything,
    dry_run: bool = False,
):
    print(f"build {dataset_name!r} full text search anything dataset ...")

    path_data = dir_afwf_fts_anything.joinpath(f"{dataset_name}-data.json")
    path_setting = dir_afwf_fts_anything.joinpath(f"{dataset_name}-setting.json")
    dir_whoosh_index = dir_afwf_fts_anything.joinpath(f"{dataset_name}-whoosh_index")

    data = list()
    for path in dir_source.glob("**/*"):
        if path.suffix not in [".rst", ".ipynb"]:
            continue
        try:
            # 提取 title 和 keywords
            title, keywords = extract_title_and_keywords(path)
            # 创建每篇文档的 URL
            url = get_url(
                domain=domain,
                dir_source=dir_source,
                path_index_rst=path,
            )
            # 构建最终被 index 的数据
            if keywords is None:
                search = title
            else:
                search = " ".join([title, " ".join(keywords)])
            row = {
                "title": title,
                "search": search,
                "url": url,
            }
            data.append(row)
        except FailedToExtractTitleAndKeywords as e:
            pass
        except UnknownIndexFileType as e:
            pass

    settings = {
        "fields": [
            {
                "name": "title",
                "type_is_store": True,
            },
            {
                "name": "search",
                "type_is_store": False,
                "type_is_ngram": True,
                "ngram_minsize": 2,
                "ngram_maxsize": 10,
            },
            {
                "name": "url",
                "type_is_store": True,
            },
        ],
        "title_field": "{title}",
        "subtitle_field": "read '{title}'",
        "arg_field": "{url}",
        "autocomplete_field": "{title}",
    }

    if dry_run is False:
        if dir_whoosh_index.exists():
            shutil.rmtree(dir_whoosh_index, ignore_errors=True)
        path_data.write_text(json.dumps(data, indent=4))
        path_setting.write_text(json.dumps(settings, indent=4))

    print(
        f"done! "
        f"check full text search data at '{path_data}' "
        f"and check full text search settings at '{path_setting}'."
    )


build_fts_anything_dataset(
    domain="https://learn-big-data-on-aws.readthedocs.io/",
    dataset_name="learnbigdata",
)
