# src/AGS324/__init__.py

"""
AGS324 - A utility library for upgrading AGS 3.1 data to AGS 4.

This package provides tools for:
- Upgrading AGS 3.1 files to AGS 4 format, with minimally required data for usage of **Autodesk Civil 3D(R)** **Geotechnical Modeler(R)**.

*Note: Autodesk Civil 3D(R), Geotechnical Modeler(R) are trademarks of Autodesk, Inc. This package is not affiliated with or endorsed by Autodesk.*

Basic usage:
    >>> from AGS324 import *
    >>> ags4_c3dgm("input.ags") # Creates "input_AGS4.ags"
"""

# Version information
__version__ = "0.1.0"
__author__ = "Yixuan Zhong"
__email__ = "yixuan.zhong.public@gmail.com"

# Import main functionality
from .main import ags4_c3dgm

# Define what gets imported with "from my_package import *"
__all__ = ['ags4_c3dgm']