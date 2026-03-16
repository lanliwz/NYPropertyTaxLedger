from __future__ import annotations

import os


DEFAULT_TAX_FILE_FOLDER = "/Users/weizhang/Downloads/tax-62n"

TAX_FILE_FOLDER = os.getenv("TAX_FILE_FOLDER", DEFAULT_TAX_FILE_FOLDER)
