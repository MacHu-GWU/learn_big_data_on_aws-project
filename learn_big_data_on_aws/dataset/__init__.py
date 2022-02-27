# -*- coding: utf-8 -*-

from . import (
    ds001,
    ds002,
)


def create_all_dataset():
    ds001.dataset.create_all()
    ds002.dataset.create_all()
