# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
from os.path import isfile


def set_default_label(label):
    if label and len(label) > 0:
        if "fi" in label:
            label["und"] = label["fi"]
        elif "en" in label:
            label["und"] = label["en"]
        else:
            label["und"] = next(iter(label.values()))


def file_exists(file_path):
    return isfile(file_path)
