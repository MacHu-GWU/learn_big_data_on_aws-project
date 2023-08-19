# -*- coding: utf-8 -*-

import subprocess
from aws_glue_container_launcher.api import (
    build_spark_submit_args,
    build_jupyter_lab_args,
    build_pytest_args,
)
from .config import config
from . import paths


def run_jupyter_lab_glue_container():
    args = build_jupyter_lab_args(
        dir_home=paths.dir_home,
        dir_workspace=paths.dir_project_root,
        boto_session=config.bsm.boto_ses,
        spark_ui_port=4041,
        spark_history_server_port=18081,
        livy_server_port=8999,
        jupyter_notebook_port=8889,
        enable_hudi=True,
        additional_docker_run_args=None,
        additional_env_vars=None,
    )
    # print("\t\n".join(args))
    subprocess.run(args)
