# AGS324 - A utility library for upgrading AGS 3.1 data to AGS 4 (and back).

This package provides tools for:
- Upgrading AGS 3.1 files to AGS 4 format, with minimally required data for usage of **Autodesk Civil 3D(R)** **Geotechnical Modeler(R)**.
- Upgrading AGS 3.1 files to a specific AGS 4 version (4.0.3, 4.0.4, 4.1, 4.1.1, 4.2).
- Downgrading AGS 4 files back to AGS 3.1, preserving optional group prefixes.

*Note: Autodesk Civil 3D(R), Geotechnical Modeler(R) are trademarks of Autodesk, Inc. This package is not affiliated with or endorsed by Autodesk.*

## Basic usage

### Import
```python
from AGS324 import ags4_c3dgm, upgrade, downgrade
```

All three conversion functions accept an optional output path as their second argument. When omitted, the output file is written next to the input with a default suffix appended (`_AGS4.ags` for upgrades, `_AGS3.ags` for downgrades).

### Upgrade an AGS 3.1 file for Civil 3D Geotechnical Modeler
```python
ags4_c3dgm("input.ags")                        # writes input_AGS4.ags
ags4_c3dgm("input.ags", "custom_output.ags")   # writes custom_output.ags
```

### Upgrade an AGS 3.1 file to a specific AGS 4 version
```python
upgrade("input.ags", version="4.1.1")                        # writes input_AGS4.ags
upgrade("input.ags", "custom_output.ags", version="4.1.1")   # writes custom_output.ags
```
If `version` is omitted, `upgrade()` targets the latest bundled AGS 4 version (`4.2`). Supported versions: `4.0.3`, `4.0.4`, `4.1`, `4.1.1`, `4.2`.

### Downgrade an AGS 4 file to AGS 3.1
```python
downgrade("input.ags")                        # writes input_AGS3.ags
downgrade("input.ags", "custom_output.ags")   # writes custom_output.ags
downgrade("input.ags", version="4.1.1")       # force a specific source-version crosswalk
```
If `version` is omitted, `downgrade()` auto-detects the source AGS 4 version from the file's `TRAN_AGS` field, falling back to the latest bundled version (`4.2`) if the field is missing or unrecognized.

## Acknowledgements

The bundled `ags4*_standard_dictionary.ags` files are sourced from the AGS Data Format Working Group's [`ags-python-library`](https://gitlab.com/ags-data-format-wg/ags-python-library).
