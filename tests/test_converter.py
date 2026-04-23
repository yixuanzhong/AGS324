import csv
import sys
import tempfile
import unittest
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
TESTDATA = ROOT / "tests" / "testdata"
TESTDATA_OUTPUT = TESTDATA / "output"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from AGS324 import ags4_c3dgm, downgrade, upgrade
from AGS324.converter import (
    AGSTable,
    _backfill_table_units_from_references,
    _infer_temporal_unit,
    _load_schema_references,
    _parse_ags_tables,
    _serialize_ags3_tables,
    _serialize_ags4_tables,
)


class ConverterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workdir = Path(self.temp_dir.name)
        TESTDATA_OUTPUT.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write(self, name: str, contents: str) -> Path:
        path = self.workdir / name
        path.write_text(contents, encoding="utf-8")
        return path

    def _read(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def _parse_output_tables(self, path: Path) -> dict[str, AGSTable]:
        return _parse_ags_tables(self._read(path))

    def _fixture_path(self, name: str) -> Path:
        return TESTDATA / name

    def _copy_fixture(self, fixture_name: str, target_name: str | None = None) -> Path:
        source = self._fixture_path(fixture_name)
        target = self.workdir / (target_name or source.name)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        return target

    def _run_upgrade_fixture(self, fixture_name: str, source_name: str | None = None) -> tuple[Path, str]:
        source = self._copy_fixture(fixture_name, source_name)
        output = TESTDATA_OUTPUT / f"{source.stem}_AGS4.ags"
        upgrade(str(source), str(output))
        return output, self._read(output)

    def _run_downgrade_fixture(self, fixture_name: str, source_name: str | None = None) -> tuple[Path, str]:
        source = self._copy_fixture(fixture_name, source_name)
        output = TESTDATA_OUTPUT / f"{source.stem}_AGS3.ags"
        downgrade(str(source), str(output))
        return output, self._read(output)

    def test_upgrade_sanitizes_and_fans_out_hole_and_clss(self) -> None:
        source = self._write(
            "input.ags",
            '\n'.join(
                [
                    '"**PROJ"',
                    '"PROJ_ID","PROJ_NAME","PROJ_DATE","PROJ_AGS","PROJ_ISNO"',
                    '"P1","Test Project","08/04/2026","AGS3.1","ISS-1"',
                    '"**HOLE"',
                    '"HOLE_ID","HOLE_TYPE","HOLE_NATE","HOLE_NATN","HOLE_GL","HOLE_FDEP","HOLE_STAR","HOLE_ENDD","HOLE_CREW","HOLE_EXC","HOLE_LOG","HOLE_REM","?HOLE_OFFS"',
                    '"BH1","BH","100.0","200.0","10.0","12.5","08/04/2026","09/04/2026","Crew A","Rig 1","Logger 1","Line 1"\n"<CONT>Line 2","1.5"',
                    '"**CLSS"',
                    '"HOLE_ID","SAMP_TOP","SAMP_REF","SAMP_TYPE","SPEC_REF","SPEC_DPTH","CLSS_LL","CLSS_PL","CLSS_PREP","CLSS_REM"',
                    '"BH1","1.0","S1","UND","SP1","1.1","50","20","Prep A","Remark A"',
                    "",
                ]
            ),
        )

        output = self.workdir / "output_AGS4.ags"
        upgrade(str(source), str(output))
        converted = self._read(output)

        self.assertIn('"GROUP","LOCA"', converted)
        self.assertIn('"GROUP","HDPH"', converted)
        self.assertIn('"GROUP","LLPL"', converted)
        self.assertIn('"GROUP","LLIN"', converted)
        self.assertIn('"GROUP","TRAN"', converted)
        self.assertIn('"TYPE","ID","PA","2DP","2DP","2DP","X","2DP","DT","DT","2DP"', converted)
        self.assertIn('"DATA","BH1","BH","100.00","200.00","10.00","Line 1Line 2","12.50","2026-04-08","2026-04-09","1.50"', converted)
        self.assertIn('"DATA","BH1","0.00","12.50","BH","2026-04-08","2026-04-09","Crew A","Rig 1","Logger 1"', converted)
        self.assertIn('"DATA","BH1","1.00","S1","UND","","SP1","1.10","50","20","Prep A","Remark A"', converted)

    def test_parse_ags3_continuation_rows_reconstruct_logical_rows(self) -> None:
        parsed = _parse_ags_tables(
            '\n'.join(
                [
                    '"**PROJ"',
                    '"*PROJ_ID","*PROJ_MEMO","*PROJ_DATE","*PROJ_AGS"',
                    '"<UNITS>","","dd/mm/yyyy",""',
                    '"P1","Lab"',
                    '"<CONT>oratory data","03/03/2015","3.1"',
                    '"**CONG"',
                    '"*HOLE_ID","*SAMP_TOP","*CONG_SATR",',
                    '"*CONG_SATH","*?CONG_IVR"',
                    '"<UNITS>","","m","%","%",""',
                    "",
                ]
            )
        )

        self.assertEqual(parsed["PROJ"].rows[0]["PROJ_MEMO"], "Laboratory data")
        self.assertEqual(parsed["PROJ"].rows[0]["PROJ_DATE"], "03/03/2015")
        self.assertEqual(parsed["PROJ"].rows[0]["PROJ_AGS"], "3.1")
        self.assertEqual(parsed["CONG"].headings, ["HOLE_ID", "SAMP_TOP", "CONG_SATR", "CONG_SATH", "CONG_IVR"])

    def test_load_schema_references_supports_each_bundled_ags4_version(self) -> None:
        expected_stats = {
            "4.0.3": (124, 2093),
            "4.0.4": (124, 2101),
            "4.1": (148, 2898),
            "4.1.1": (148, 2895),
            "4.2": (171, 3412),
        }

        self.assertEqual(_load_schema_references().ags4_version, "4.2")
        for version, (expected_groups, expected_headings) in expected_stats.items():
            references = _load_schema_references(version)
            self.assertEqual(references.ags4_version, version)
            self.assertEqual(len(references.ags4_groups), expected_groups)
            self.assertEqual(sum(len(headings) for headings in references.ags4_headings.values()), expected_headings)

    def test_load_schema_references_rejects_unknown_ags4_version(self) -> None:
        message = re.escape("Supported versions: 4.0.3, 4.0.4, 4.1, 4.1.1, 4.2")
        with self.assertRaisesRegex(ValueError, message):
            _load_schema_references("4.2.0")

    def test_backfill_missing_units_from_reference_for_ags3_and_ags4(self) -> None:
        references = _load_schema_references()
        parsed = _parse_ags_tables(
            '\n'.join(
                [
                    '"**SAMP"',
                    '"*HOLE_ID","*SAMP_TOP","*SAMP_REF","*SAMP_TYPE","*SAMP_DIA","*SAMP_BASE","*SAMP_DESC","*SAMP_REM"',
                    '"<UNITS>","m","","","","m","",""',
                    '"BH1","18","1","TW","75","19","Desc","95"',
                    "",
                    '"GROUP","LOCA"',
                    '"HEADING","LOCA_ID","LOCA_NATE","LOCA_NATN"',
                    '"UNIT","","","m"',
                    '"TYPE","ID","2DP","2DP"',
                    '"DATA","BH1","123.45","678.90"',
                    "",
                ]
            )
        )

        _backfill_table_units_from_references(parsed, references)

        self.assertEqual(parsed["SAMP"].units, ["", "m", "", "", "mm", "m", "", ""])
        self.assertEqual(parsed["LOCA"].units, ["", "m", "m"])

    def test_infer_temporal_unit_selects_content_specific_patterns(self) -> None:
        self.assertEqual(_infer_temporal_unit(["2026-04-08T12:34:56.789+08:00"], {"dataType": "DT"}), "yyyy-mm-ddThh:mm:ss.sssZ(+hh:mm)")
        self.assertEqual(_infer_temporal_unit(["2026-04-08"], {"dataType": "DT"}), "yyyy-mm-dd")
        self.assertEqual(_infer_temporal_unit(["12:34:56"], {"dataType": "DT"}), "hh:mm:ss")
        self.assertEqual(_infer_temporal_unit(["2026"], {"dataType": "DT"}), "yyyy")
        self.assertEqual(_infer_temporal_unit(["08/04/2026"], {"type": "Date"}), "dd/mm/yyyy")
        self.assertEqual(_infer_temporal_unit(["1234"], {"type": "Time"}), "hhmm")

    def test_upgrade_prefers_source_units_and_falls_back_to_reference(self) -> None:
        source = self._write(
            "units_upgrade.ags",
            '\n'.join(
                [
                    '"**PROJ"',
                    '"*PROJ_ID","*PROJ_NAME","*PROJ_DATE","*PROJ_AGS"',
                    '"<UNITS>","","dd/mm/yyyy",""',
                    '"P1","Project","08/04/2026","3.1"',
                    '"**HOLE"',
                    '"*HOLE_ID","*HOLE_TYPE","*HOLE_NATE","*HOLE_NATN","*HOLE_GL","*HOLE_FDEP","*HOLE_STAR","*HOLE_ENDD"',
                    '"<UNITS>","","ft","ft","","ft","dd/mm/yyyy","dd/mm/yyyy"',
                    '"BH1","BH","100","200","10","12.5","08/04/2026","09/04/2026"',
                    "",
                ]
            ),
        )

        output = self.workdir / "units_upgrade_AGS4.ags"
        upgrade(str(source), str(output))
        converted = self._read(output)

        self.assertIn('"HEADING","LOCA_ID","LOCA_TYPE","LOCA_NATE","LOCA_NATN","LOCA_GL","LOCA_FDEP","LOCA_STAR","LOCA_ENDD"', converted)
        self.assertIn('"UNIT","","","ft","ft","m","ft","yyyy-mm-dd","yyyy-mm-dd"', converted)
        self.assertIn('"HEADING","LOCA_ID","HDPH_TOP","HDPH_BASE","HDPH_TYPE","HDPH_STAR","HDPH_ENDD"', converted)
        self.assertIn('"UNIT","","ft","ft","","yyyy-mm-dd","yyyy-mm-dd"', converted)
        self.assertIn('"HEADING","TRAN_DATE","TRAN_AGS"', converted)
        self.assertIn('"UNIT","yyyy-mm-dd",""', converted)

    def test_downgrade_prefers_source_units_and_infers_temporal_units(self) -> None:
        source = self._write(
            "units_downgrade.ags",
            '\n'.join(
                [
                    '"GROUP","PROJ"',
                    '"HEADING","PROJ_ID","PROJ_NAME","PROJ_LOC","PROJ_CLNT","PROJ_CONT","PROJ_ENG"',
                    '"UNIT","","","","","",""',
                    '"TYPE","ID","X","X","X","X","X"',
                    '"DATA","P1","Project","","","",""',
                    '"GROUP","TRAN"',
                    '"HEADING","TRAN_ISNO","TRAN_DATE","TRAN_PROD","TRAN_STAT","TRAN_AGS","TRAN_RECV"',
                    '"UNIT","","yyyy-mm-dd","","","",""',
                    '"TYPE","X","DT","X","X","X","X"',
                    '"DATA","","2026-04-08","","Final","4",""',
                    '"GROUP","LOCA"',
                    '"HEADING","LOCA_ID","LOCA_TYPE","LOCA_NATE","LOCA_NATN","LOCA_GL","LOCA_FDEP","LOCA_STAR","LOCA_ENDD"',
                    '"UNIT","","","ft","ft","","ft","yyyy-mm-dd","yyyy-mm-dd"',
                    '"TYPE","ID","PA","2DP","2DP","2DP","2DP","DT","DT"',
                    '"DATA","BH1","BH","100","200","10","12.5","2026-04-08","2026-04-09"',
                    '"GROUP","PTIM"',
                    '"HEADING","LOCA_ID","PTIM_DTIM","PTIM_DPTH"',
                    '"UNIT","","yyyy-mm-ddThh:mm:ss.sssZ(+hh:mm)","ft"',
                    '"TYPE","ID","DT","2DP"',
                    '"DATA","BH1","2026-04-08T12:34:56","3.5"',
                    "",
                ]
            ),
        )

        output = self.workdir / "units_downgrade_AGS3.ags"
        downgrade(str(source), str(output))
        converted = self._read(output)

        self.assertIn('"**HOLE"', converted)
        self.assertIn('"<UNITS>","","ft","ft","m","ft","dd/mm/yyyy","dd/mm/yyyy"', converted)
        self.assertIn('"**PTIM"', converted)
        self.assertIn('"<UNITS>","dd/mm/yyyy","hhmmss","ft"', converted)
        self.assertIn('"08/04/2026"', converted)
        self.assertIn('"123456"', converted)

    def test_upgrade_uses_exact_requested_ags4_version_in_tran_ags(self) -> None:
        source = self._write(
            "version_target.ags",
            '\n'.join(
                [
                    '"**PROJ"',
                    '"*PROJ_ID","*PROJ_NAME","*PROJ_DATE","*PROJ_AGS"',
                    '"<UNITS>","","","dd/mm/yyyy",""',
                    '"P1","Version Target","08/04/2026","3.1"',
                    "",
                ]
            ),
        )

        cases = [
            (None, "4.2"),
            ("4.1.1", "4.1.1"),
            ("4.0.3", "4.0.3"),
        ]
        for requested_version, expected_version in cases:
            output = self.workdir / f"version_target_{expected_version.replace('.', '_')}_AGS4.ags"
            if requested_version is None:
                upgrade(str(source), str(output))
            else:
                upgrade(str(source), str(output), version=requested_version)
            parsed = self._parse_output_tables(output)
            self.assertEqual(parsed["TRAN"].rows[0]["TRAN_AGS"], expected_version)

    def test_upgrade_version_specific_crosswalk_changes_output_groups(self) -> None:
        source = self._write(
            "iprm_versioned.ags",
            '\n'.join(
                [
                    '"**IPRM"',
                    '"HOLE_ID","IPRM_TOP","IPRM_BASE","IPRM_TESN","IPRM_STG","IPRM_FLOW","IPRM_HEAD","IPRM_IPRM"',
                    '"BH1","1.0","2.0","T1","Rising","5.2","10.5","Pipe 1"',
                    "",
                ]
            ),
        )

        output_403 = self.workdir / "iprm_4_0_3_AGS4.ags"
        output_42 = self.workdir / "iprm_4_2_AGS4.ags"
        upgrade(str(source), str(output_403), version="4.0.3")
        upgrade(str(source), str(output_42), version="4.2")

        parsed_403 = self._parse_output_tables(output_403)
        parsed_42 = self._parse_output_tables(output_42)
        self.assertNotIn("FGHG", parsed_403)
        self.assertNotIn("FGHS", parsed_403)
        self.assertIn("FGHG", parsed_42)
        self.assertIn("FGHS", parsed_42)

    def test_serialize_ags3_wraps_heading_and_data_rows_to_240_chars(self) -> None:
        references = _load_schema_references()
        heading_table = AGSTable(
            group="SAMP",
            headings=[f"SAMP_HEADING_{index:02d}_EXTREMELY_LONG_TOKEN" for index in range(1, 16)],
            units=["" for _ in range(15)],
            rows=[],
        )
        wrapped_heading = _serialize_ags3_tables([heading_table], references)
        heading_lines = wrapped_heading.splitlines()
        starred_lines = [line for line in heading_lines[1:] if line.startswith('"*')]
        self.assertGreaterEqual(len(starred_lines), 2)
        self.assertNotIn('"<CONT>"', "\n".join(starred_lines))
        self.assertTrue(starred_lines[0].endswith(","))
        self.assertFalse(starred_lines[-1].endswith(","))
        self.assertTrue(all(len(line) <= 240 for line in heading_lines))

        data_table = AGSTable(
            group="PROJ",
            headings=["PROJ_ID", "PROJ_NAME", "PROJ_MEMO", "PROJ_DATE", "PROJ_AGS"],
            units=["", "", "", "dd/mm/yyyy", ""],
            rows=[
                {
                    "PROJ_ID": "P1",
                    "PROJ_NAME": "Project",
                    "PROJ_MEMO": "Laboratory " + ("data " * 40),
                    "PROJ_DATE": "03/03/2015",
                    "PROJ_AGS": "3.1",
                }
            ],
        )
        wrapped_data = _serialize_ags3_tables([data_table], references)
        data_lines = wrapped_data.splitlines()
        physical_rows = [
            next(csv.reader([line]))
            for line in data_lines
            if line.startswith('"P1"') or line.startswith('"<CONT>"')
        ]
        self.assertGreaterEqual(len(physical_rows), 2)
        self.assertEqual(physical_rows[0][0], "P1")
        self.assertEqual(physical_rows[1][0], "<CONT>")
        self.assertEqual(len(physical_rows[0]), 5)
        self.assertTrue(all(len(row) == 5 for row in physical_rows))
        self.assertNotEqual(physical_rows[0][2], "")
        self.assertEqual(physical_rows[1][1], "")
        self.assertEqual(len(next(line for line in data_lines if line.startswith('"P1"'))), 240)
        self.assertTrue(all(len(line) <= 240 for line in data_lines))

    def test_serialize_ags3_uses_rule_18_units_shape_and_preserves_trailing_shape(self) -> None:
        references = _load_schema_references()
        ags3 = _serialize_ags3_tables(
            [
                AGSTable(
                    group="WSTK",
                    headings=["HOLE_ID", "WSTK_DEP", "WSTK_NMIN"],
                    units=["", "m", "mm"],
                    rows=[{"HOLE_ID": "TP2-05", "WSTK_DEP": "3.3", "WSTK_NMIN": ""}],
                ),
                AGSTable(
                    group="ABBR",
                    headings=["ABBR_HDNG", "ABBR_CODE", "ABBR_DESC"],
                    units=["", "", ""],
                    rows=[{"ABBR_HDNG": "DICT_TYPE", "ABBR_CODE": "HEADING", "ABBR_DESC": "Heading"}],
                ),
                AGSTable(
                    group="DICT",
                    headings=["DICT_TYPE", "DICT_GRP", "DICT_HDNG", "DICT_STAT", "DICT_DESC", "DICT_UNIT", "DICT_EXMP", "DICT_PGRP"],
                    units=["", "", "", "", "", "", "", ""],
                    rows=[
                        {
                            "DICT_TYPE": "HEADING",
                            "DICT_GRP": "GEOL",
                            "DICT_HDNG": "GEOL_GEO3",
                            "DICT_STAT": "OTHER",
                            "DICT_DESC": "Third geology code",
                            "DICT_UNIT": "",
                            "DICT_EXMP": "",
                            "DICT_PGRP": "",
                        }
                    ],
                ),
            ],
            references,
        )

        self.assertIn('"**WSTK"\n"*HOLE_ID","*WSTK_DEP","*WSTK_NMIN"\n"<UNITS>","m","mm"\n"TP2-05","3.3",""', ags3)
        self.assertNotIn('"**WSTK"\n"*HOLE_ID","*WSTK_DEP","*WSTK_NMIN"\n"<UNITS>","","m","mm"', ags3)
        self.assertNotIn('"**ABBR"\n"*ABBR_HDNG","*ABBR_CODE","*ABBR_DESC"\n"<UNITS>"', ags3)
        self.assertNotIn('"**DICT"\n"*DICT_TYPE","*DICT_GRP","*DICT_HDNG","*DICT_STAT","*DICT_DESC","*DICT_UNIT","*DICT_EXMP","*?DICT_PGRP"\n"<UNITS>"', ags3)
        self.assertIn('"HEADING","GEOL","GEOL_GEO3","OTHER","Third geology code","","",""', ags3)
        self.assertTrue(ags3.endswith("\n\n"))

    def test_serialize_ags4_preserves_trailing_shape_and_uses_crlf(self) -> None:
        ags4 = _serialize_ags4_tables(
            [
                AGSTable(
                    group="DICT",
                    headings=["DICT_TYPE", "DICT_GRP", "DICT_HDNG", "DICT_STAT", "DICT_DESC", "DICT_UNIT", "DICT_EXMP", "DICT_PGRP"],
                    units=["", "", "", "", "", "", "", ""],
                    types=["X", "X", "X", "X", "X", "X", "X", "X"],
                    rows=[
                        {
                            "DICT_TYPE": "HEADING",
                            "DICT_GRP": "GEOL",
                            "DICT_HDNG": "GEOL_GEO3",
                            "DICT_STAT": "OTHER",
                            "DICT_DESC": "Third geology code",
                            "DICT_UNIT": "",
                            "DICT_EXMP": "",
                            "DICT_PGRP": "",
                        }
                    ],
                )
            ],
            include_types=True,
        )

        self.assertIn('"DATA","HEADING","GEOL","GEOL_GEO3","OTHER","Third geology code","","",""\r\n', ags4)
        self.assertIn('\r\n\r\n', ags4)
        self.assertTrue(ags4.endswith('\r\n\r\n'))

    def test_downgrade_rebuilds_hole_and_blanks_conflicts(self) -> None:
        source = self._write(
            "input_AGS4.ags",
            '\n'.join(
                [
                    '"GROUP","HORN"',
                    '"HEADING","LOCA_ID","HORN_TOP","HORN_BASE","HORN_ORNT","HORN_INCL","HORN_REM"',
                    '"UNIT","","m","m","deg","deg",""',
                    '"TYPE","ID","2DP","2DP","0DP","0DP","X"',
                    '"DATA","BH1","0.0","12.5","90","5","Orientation note"',
                    '"GROUP","LOCA"',
                    '"HEADING","LOCA_ID","LOCA_TYPE","LOCA_NATE","LOCA_NATN","LOCA_GL","LOCA_FDEP","LOCA_STAR","LOCA_ENDD","LOCA_ETRV","LOCA_NTRV"',
                    '"UNIT","","","m","m","m","m","yyyy-mm-dd","yyyy-mm-dd","m","m"',
                    '"TYPE","ID","PA","2DP","2DP","2DP","2DP","DT","DT","2DP","2DP"',
                    '"DATA","BH1","BH","100.0","200.0","10.0","12.5","2026-04-08","2026-04-09","300.0","400.0"',
                    '"GROUP","LLPL"',
                    '"HEADING","LOCA_ID","SAMP_TOP","SAMP_REF","SAMP_TYPE","SAMP_ID","SPEC_REF","SPEC_DPTH","LLPL_LL","LLPL_PL","LLPL_PREP","LLPL_REM"',
                    '"UNIT","","m","","","","","m","","","",""',
                    '"TYPE","ID","2DP","ID","PA","ID","ID","2DP","2DP","2DP","X","X"',
                    '"DATA","BH1","1.0","S1","UND","","SP1","1.1","50","20","Prep A","Remark A"',
                    '"GROUP","LVAN"',
                    '"HEADING","LOCA_ID","SAMP_TOP","SAMP_REF","SAMP_TYPE","SAMP_ID","SPEC_REF","SPEC_DPTH","LVAN_VNPK","LVAN_VNRM"',
                    '"UNIT","","m","","","","","m","",""',
                    '"TYPE","ID","2DP","ID","PA","ID","ID","2DP","2DP","2DP"',
                    '"DATA","BH1","1.0","S1","UND","","SP1","1.1","55","Different remark"',
                    "",
                ]
            ),
        )

        output = self.workdir / "output_AGS3.ags"
        downgrade(str(source), str(output))
        converted = self._read(output)

        self.assertIn('"**HOLE"', converted)
        self.assertIn('"**CLSS"', converted)
        self.assertIn('"**?HORN"', converted)
        self.assertNotIn('"GROUP","HOLE"', converted)
        self.assertIn('"BH1","BH","100.0","200.0","10.0","12.5","08/04/2026","300.0","400.0","09/04/2026"', converted)
        self.assertIn('"*?HORN_ORNT","*?HORN_INCL","*?HORN_REM"', converted)
        self.assertIn('"BH1","0.0","12.5","90","5","Orientation note"', converted)
        self.assertIn('"BH1","1.0","S1","UND","SP1","1.1","50","20","Prep A","55","Different remark","55","Different remark","Remark A"', converted)

    def test_upgrade_drops_unmapped_fields(self) -> None:
        source = self._write(
            "drop_test.ags",
            '\n'.join(
                [
                    '"**PROJ"',
                    '"PROJ_ID","PROJ_CID","PROJ_NAME"',
                    '"P2","UNMAPPED","Project Two"',
                    "",
                ]
            ),
        )

        output = self.workdir / "drop_test_AGS4.ags"
        upgrade(str(source), str(output))
        converted = self._read(output)

        self.assertIn('"GROUP","PROJ"', converted)
        self.assertNotIn("PROJ_CID", converted)
        self.assertIn('"DATA","P2","Project Two"', converted)

    def test_upgrade_and_downgrade_default_suffixes(self) -> None:
        source = self._write(
            "default.ags",
            '\n'.join(
                [
                    '"**PROJ"',
                    '"PROJ_ID","PROJ_NAME"',
                    '"P3","Suffix Test"',
                    "",
                ]
            ),
        )

        upgrade(str(source))
        downgraded_source = self.workdir / "default_AGS4.ags"
        self.assertTrue(downgraded_source.exists())

        downgrade(str(downgraded_source))
        self.assertTrue((self.workdir / "default_AGS4_AGS3.ags").exists())

    def test_ags4_c3dgm_regression_path_still_works(self) -> None:
        source = self._write(
            "legacy.ags",
            '\n'.join(
                [
                    '"**PROJ"',
                    '"PROJ_ID","PROJ_NAME"',
                    '"P4","Legacy"',
                    '"**ABBR"',
                    '"ABBR_CODE","ABBR_DESC","ABBR_HDNG"',
                    '"A","B","C"',
                    '"**DICT"',
                    '"DICT_GRP","DICT_HDNG","DICT_TYPE","DICT_UNIT","DICT_DESC","DICT_STAT","DICT_EXMP","DICT_PGRP"',
                    '"PROJ","PROJ_ID","X","","Project id","","",""',
                    '"**UNIT"',
                    '"UNIT_UNIT","UNIT_DESC"',
                    '"m","metre"',
                    '"**GEOL"',
                    '"HOLE_ID","GEOL_TOP","GEOL_BASE","GEOL_DESC"',
                    '"BH1","0.0","1.0","Made Ground"',
                    '"**ISPT"',
                    '"HOLE_ID","ISPT_TOP","ISPT_BASE","ISPT_NVAL"',
                    '"BH1","1.0","1.5","10"',
                    '"**HOLE"',
                    '"HOLE_ID","HOLE_TYPE","HOLE_FDEP","HOLE_STAR","HOLE_ENDD","HOLE_CREW","HOLE_EXC","HOLE_LOG","HOLE_REM"',
                    '"BH1","BH","12.5","08/04/2026","09/04/2026","Crew A","Rig 1","Logger 1","Legacy note"',
                    "",
                ]
            ),
        )

        output = self.workdir / "legacy_AGS4.ags"
        ags4_c3dgm(str(source), str(output))
        converted = self._read(output)

        self.assertIn('"GROUP","LOCA"', converted)
        self.assertIn('"GROUP","HDPH"', converted)

    def test_upgrade_real_testdata_to_upgrade_applies_targeted_ags4_normalization(self) -> None:
        output, converted = self._run_upgrade_fixture("to_upgrade.AGS", "to_upgrade.ags")
        raw_bytes = output.read_bytes()

        self.assertTrue(output.exists())
        self.assertIn('"GROUP","PROJ"', converted)
        self.assertIn('"GROUP","TRAN"', converted)
        self.assertIn('"TYPE"', converted)
        self.assertIn('"DATA","2015-03-03","Final","4.2"', converted)
        self.assertNotRegex(converted, r'"HEADING"[^\n]*\*[?A-Z_]+')
        self.assertNotIn('"DATA","*CONG_SATH"', converted)
        self.assertIn("Laboratory data", converted)
        self.assertNotIn("Lab\noratory", converted)
        self.assertRegex(converted, r'"DATA"[^\n]*"44\.00"[^\n]*')
        self.assertRegex(converted, r'"DATA"[^\n]*"0\.00200"[^\n]*')
        self.assertRegex(converted, r'"DATA"[^\n]*"98\.33"[^\n]*')
        self.assertIn(b"\r\n\r\n\"GROUP\",\"ABBR\"", raw_bytes)
        self.assertTrue(raw_bytes.endswith(b"\r\n\r\n"))

    def test_downgrade_real_testdata_to_downgrade_emits_legacy_ags3_layout(self) -> None:
        output, converted = self._run_downgrade_fixture("to_downgrade.ags")

        self.assertTrue(output.exists())
        self.assertRegex(converted, r'(?m)^"\*\*PROJ"$')
        self.assertRegex(converted, r'(?m)^"\*PROJ_ID".*"\*\?PROJ_STAT".*"\*PROJ_AGS".*$')
        self.assertNotRegex(converted, r'(?m)^"GROUP"')
        self.assertNotRegex(converted, r'(?m)^"DATA"')
        self.assertIn('"3.1"', converted)
        self.assertIn('"**PROJ"', converted)
        self.assertIn('"**ABBR"', converted)
        self.assertIn('"**?BKFL"', converted)
        self.assertIn('"**DICT"', converted)
        self.assertIn('"**FILE"', converted)
        self.assertIn('"<CONT>"', converted)
        self.assertTrue(all(len(line) <= 240 for line in converted.splitlines()))
        self.assertIn('\n\n"**ABBR"', converted)
        self.assertIn('\n\n"**DICT"', converted)
        self.assertTrue(converted.endswith("\n\n"))
        self.assertIn('"**WSTK"\n"*HOLE_ID","*WSTK_DEP","*WSTK_NMIN"\n"<UNITS>","m","mm"', converted)
        self.assertNotIn('"**WSTK"\n"*HOLE_ID","*WSTK_DEP","*WSTK_NMIN"\n"<UNITS>","","m","mm"', converted)
        self.assertNotIn('"**ABBR"\n"*ABBR_HDNG","*ABBR_CODE","*ABBR_DESC"\n"<UNITS>"', converted)
        self.assertNotIn('"**DICT"\n"*DICT_TYPE","*DICT_GRP","*DICT_HDNG","*DICT_STAT","*DICT_DESC","*DICT_UNIT"\n"<UNITS>"', converted)

    def test_real_testdata_round_trip_small_fixture_completes(self) -> None:
        downgraded_path, downgraded = self._run_downgrade_fixture("to_downgrade.ags")

        self.assertTrue(downgraded_path.exists())
        roundtrip_output = TESTDATA_OUTPUT / "to_downgrade_roundtrip_AGS4.ags"
        upgrade(str(downgraded_path), str(roundtrip_output))
        reconverted = self._read(roundtrip_output)

        self.assertTrue(roundtrip_output.exists())
        self.assertIn('"GROUP","PROJ"', reconverted)
        self.assertIn('"GROUP","TRAN"', reconverted)
        self.assertRegex(reconverted, r'"DATA"[^\n]*"4\.2"[^\n]*')

    def test_versioned_crosswalk_targets_exist_in_matching_ags4_references(self) -> None:
        ref_dir = SRC / "AGS324" / "ref"
        versions = ("4.0.3", "4.0.4", "4.1", "4.1.1", "4.2")

        for version in versions:
            references = _load_schema_references(version)
            crosswalk_path = ref_dir / f"ags3-to-ags{version}-semantic-crosswalk.csv"
            self.assertTrue(crosswalk_path.exists())
            with crosswalk_path.open(encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    self.assertIn(row["AGS4_GROUP"], references.ags4_headings)
                    self.assertIn(row["AGS4_HEADING"], references.ags4_headings[row["AGS4_GROUP"]])


if __name__ == "__main__":
    unittest.main()
