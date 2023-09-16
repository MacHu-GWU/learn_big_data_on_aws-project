# -*- coding: utf-8 -*-

"""
Warning! This installation script is like how those popular opensource project
automate the installation:

- `Oh-my-zsh <https://github.com/ohmyzsh/ohmyzsh/blob/master/oh-my-zsh.sh>`_
- `HomeBrew <https://github.com/Homebrew/install/blob/master/install.sh>`_
- `pyenv installer <https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer>`_

If this installation script is hacked, it may harm your laptop.

Usage:

.. code-block:: bash

    python3 -c "$(curl -fsSL https://raw.githubusercontent.com/MacHu-GWU/learn_big_data_on_aws-project/main/install.py)"
"""

import sys
import shutil
import subprocess
from pathlib import Path

# Define variables
# don't use Path(__file__).absolute().parent, this script is executed by `python3 -c (curl ...)`
dir_here = Path.cwd().absolute()
dir_tmp = dir_here.joinpath("tmp")
dir_repo = dir_tmp.joinpath("learn_big_data_on_aws-project")

dir_tmp.mkdir(exist_ok=True)

if dir_repo.exists():
    shutil.rmtree(str(dir_repo))

args = [
    "git",
    "clone",
    "https://github.com/MacHu-GWU/learn_big_data_on_aws-project",
    f"{dir_repo}"
]
subprocess.run(args)

args = [
    sys.executable,
    str(dir_tmp.joinpath("learn_big_data_on_aws-project", "build_fts_anything_dataset.py")),
]
subprocess.run(args)
