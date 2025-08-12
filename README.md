# AGS324 - A utility library for upgrading AGS 3.1 data to AGS 4.

This package provides tools for:
- Upgrading AGS 3.1 files to AGS 4 format, with minimally required data for usage of **Autodesk Civil 3D(R)** **Geotechnical Modeler(R)**.

*Note: Autodesk Civil 3D(R), Geotechnical Modeler(R) are trademarks of Autodesk, Inc. This package is not affiliated with or endorsed by Autodesk.*

## Basic usage:
### Import
`from AGS324 import *`
### Upgrade an AGS 3.1 file to AGS 4
`AGS324.ags4_c3dgm("input.ags")`

or

`AGS324.ags4_c3dgm("input.ags", "input_AGS4.ags")`

Either will save the converted file as "input_AGS4.ags".
