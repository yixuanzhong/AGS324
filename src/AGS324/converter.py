from __future__ import annotations

import csv
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)

SUPPORTED_AGS4_VERSIONS: Tuple[str, ...] = ("4.0.3", "4.0.4", "4.1", "4.1.1", "4.2")
LATEST_AGS4_VERSION = SUPPORTED_AGS4_VERSIONS[-1]


def _normalize_code(value: str) -> str:
    return value.strip().lstrip("?*").strip()


def _normalize_row_label(value: str) -> str:
    return _normalize_code(value).upper()


@dataclass
class AGSTable:
    group: str
    headings: List[str]
    units: List[str] = field(default_factory=list)
    source_units: List[str] = field(default_factory=list)
    types: List[str] = field(default_factory=list)
    rows: List[Dict[str, str]] = field(default_factory=list)
    layout: str = "ags4"


@dataclass
class RowRecord:
    values: Dict[str, str] = field(default_factory=dict)
    conflicts: set[str] = field(default_factory=set)


@dataclass
class TemporalUnitShape:
    family: str
    style: str
    precision: int
    has_fraction: bool = False
    has_timezone: bool = False


@dataclass
class SchemaReferences:
    ags3_groups: List[str]
    ags3_group_meta: Dict[str, Dict[str, object]]
    ags3_headings: Dict[str, Dict[str, Dict[str, str]]]
    ags3_keys: Dict[str, List[str]]
    ags4_version: str
    ags4_groups: List[str]
    ags4_headings: Dict[str, Dict[str, Dict[str, str]]]
    ags4_keys: Dict[str, List[str]]
    forward_crosswalk: Dict[Tuple[str, str], List[Tuple[str, str]]]
    reverse_crosswalk: Dict[Tuple[str, str], List[Tuple[str, str]]]


def _resolve_reference_path(filename: str) -> Path:
    path = Path(__file__).resolve().parent / "ref" / filename
    if path.exists():
        return path
    raise FileNotFoundError(f"Packaged reference file not found: {filename}")


def _resolve_ags4_version(version: Optional[str] = None) -> str:
    resolved = version or LATEST_AGS4_VERSION
    if resolved not in SUPPORTED_AGS4_VERSIONS:
        supported = ", ".join(SUPPORTED_AGS4_VERSIONS)
        raise ValueError(f"Unsupported AGS4 version '{resolved}'. Supported versions: {supported}")
    return resolved


def _normalize_ags3_headings_by_group(raw: Dict[str, Dict[str, Dict[str, str]]]) -> Dict[str, Dict[str, Dict[str, str]]]:
    normalized: Dict[str, Dict[str, Dict[str, str]]] = {}
    for group, headings in raw.items():
        group_code = _normalize_code(group)
        normalized[group_code] = {}
        for heading, meta in headings.items():
            heading_code = _normalize_code(heading)
            normalized[group_code][heading_code] = dict(meta)
    return normalized


def _normalize_ags3_groups(raw: Dict[str, Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    normalized: Dict[str, Dict[str, object]] = {}
    for group, meta in raw.items():
        normalized[_normalize_code(group)] = dict(meta)
    return normalized


def _normalize_ags3_keys(raw: Dict[str, List[Dict[str, str]]]) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {}
    for group, keys in raw.items():
        normalized[_normalize_code(group)] = [_normalize_code(key["field"]) for key in keys]
    return normalized


def _normalize_ags4_headings_by_group(raw: Dict[str, Dict[str, Dict[str, str]]]) -> Dict[str, Dict[str, Dict[str, str]]]:
    normalized: Dict[str, Dict[str, Dict[str, str]]] = {}
    for group, headings in raw.items():
        group_code = _normalize_code(group)
        normalized[group_code] = {}
        for heading, meta in headings.items():
            heading_code = _normalize_code(heading)
            normalized[group_code][heading_code] = dict(meta)
    return normalized


def _normalize_ags4_keys(raw: Dict[str, Dict[str, Dict[str, str]]]) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {}
    for group, headings in raw.items():
        group_code = _normalize_code(group)
        normalized[group_code] = [
            _normalize_code(heading)
            for heading, meta in headings.items()
            if meta.get("status") == "KEY"
        ]
    return normalized


@lru_cache(maxsize=None)
def _load_schema_references(version: Optional[str] = None) -> SchemaReferences:
    ags4_version = _resolve_ags4_version(version)
    ags3_path = _resolve_reference_path("ags3.references.json")
    ags4_path = _resolve_reference_path(f"ags{ags4_version}.references.json")
    crosswalk_path = _resolve_reference_path(f"ags3-to-ags{ags4_version}-semantic-crosswalk.csv")

    with ags3_path.open(encoding="utf-8") as handle:
        ags3_raw = json.load(handle)
    with ags4_path.open(encoding="utf-8") as handle:
        ags4_raw = json.load(handle)

    ags3_groups = [_normalize_code(group) for group in ags3_raw["groups"].keys()]
    ags3_group_meta = _normalize_ags3_groups(ags3_raw["groups"])
    ags4_groups = [_normalize_code(group) for group in ags4_raw["groups"].keys()]
    ags3_headings = _normalize_ags3_headings_by_group(ags3_raw["headingsByGroup"])
    ags3_keys = _normalize_ags3_keys(ags3_raw.get("keysByGroup", {}))
    ags4_headings = _normalize_ags4_headings_by_group(ags4_raw["headingsByGroup"])
    ags4_keys = _normalize_ags4_keys(ags4_raw["headingsByGroup"])

    forward_crosswalk: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    reverse_crosswalk: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    with crosswalk_path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_group = _normalize_code(row["AGS3_GROUP"])
            source_heading = _normalize_code(row["AGS3_HEADING"])
            target_group = _normalize_code(row["AGS4_GROUP"])
            target_heading = _normalize_code(row["AGS4_HEADING"])
            if not (source_group and source_heading and target_group and target_heading):
                continue
            forward_crosswalk.setdefault((source_group, source_heading), []).append((target_group, target_heading))
            reverse_crosswalk.setdefault((target_group, target_heading), []).append((source_group, source_heading))

    return SchemaReferences(
        ags3_groups=ags3_groups,
        ags3_group_meta=ags3_group_meta,
        ags3_headings=ags3_headings,
        ags3_keys=ags3_keys,
        ags4_version=ags4_version,
        ags4_groups=ags4_groups,
        ags4_headings=ags4_headings,
        ags4_keys=ags4_keys,
        forward_crosswalk=forward_crosswalk,
        reverse_crosswalk=reverse_crosswalk,
    )


def _sanitize_ags_text(text: str) -> str:
    sanitized = text.replace("\ufeff", "").replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    sanitized = sanitized.replace('"<UNITS>"', '"UNIT",""').replace("<UNITS>", "UNIT")
    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
    return sanitized.strip() + "\n"


def _read_csv_rows(text: str) -> List[List[str]]:
    reader = csv.reader(StringIO(text), skipinitialspace=True)
    rows: List[List[str]] = []
    for row in reader:
        if not row:
            continue
        cleaned = [cell.strip() for cell in row]
        if any(cell for cell in cleaned):
            rows.append(cleaned)
    return rows


def _align_values(values: List[str], expected_length: int) -> List[str]:
    if len(values) < expected_length:
        return values + [""] * (expected_length - len(values))
    if len(values) > expected_length:
        overflow = values[expected_length - 1 :]
        return values[: expected_length - 1] + [",".join(overflow)]
    return values


def _trim_trailing_empty_cells(values: List[str]) -> List[str]:
    trimmed = list(values)
    while len(trimmed) > 1 and trimmed[-1] == "":
        trimmed.pop()
    return trimmed


def _is_legacy_group_row(value: str) -> bool:
    return value.strip().startswith("**")


def _is_legacy_heading_row(value: str) -> bool:
    stripped = value.strip()
    return stripped.startswith("*") and not stripped.startswith("**")


def _is_legacy_units_row(value: str) -> bool:
    return value.strip().upper() == "<UNITS>"


def _is_legacy_continuation_row(value: str) -> bool:
    return value.strip().upper().startswith("<CONT>")


def _merge_continuation_cells(existing: Dict[str, str], headings: List[str], values: List[str]) -> None:
    for heading, value in zip(headings, _align_values(values, len(headings))):
        if not value:
            continue
        current = existing.get(heading, "")
        existing[heading] = f"{current}\n{value}" if current else value


def _merge_ags3_continuation_row(
    existing: Dict[str, str],
    headings: List[str],
    raw_row: List[str],
    previous_width: int,
) -> int:
    if not existing or not headings:
        return previous_width

    first_cell = raw_row[0].strip()
    upper_first = first_cell.upper()
    highest_column = -1

    if upper_first == "<CONT>":
        non_empty_indices = [index for index, value in enumerate(raw_row[1 : len(headings)], start=1) if value]
        if not non_empty_indices:
            return max(previous_width, min(len(raw_row), len(headings)))
        start_column = non_empty_indices[0]
        continued_fragment = raw_row[start_column]
        existing[headings[start_column]] = existing.get(headings[start_column], "") + continued_fragment
        highest_column = start_column
        for index in range(start_column + 1, min(len(raw_row), len(headings))):
            value = raw_row[index]
            if not value:
                continue
            existing[headings[index]] = value
            highest_column = index
        return max(previous_width, highest_column + 1 if highest_column >= 0 else previous_width)

    if upper_first.startswith("<CONT>"):
        populated_columns = [index for index, heading in enumerate(headings) if existing.get(heading, "") != ""]
        start_column = populated_columns[-1] if populated_columns else max(previous_width - 1, 0)
        fragment = first_cell[len("<CONT>") :]
        if start_column < len(headings):
            existing[headings[start_column]] = existing.get(headings[start_column], "") + fragment
            highest_column = start_column
        for offset, value in enumerate(raw_row[1:], start=1):
            target_column = start_column + offset
            if target_column >= len(headings) or not value:
                continue
            existing[headings[target_column]] = value
            highest_column = target_column
        return max(previous_width, highest_column + 1 if highest_column >= 0 else previous_width)

    return previous_width


def _parse_ags_tables(text: str) -> Dict[str, AGSTable]:
    tables: Dict[str, AGSTable] = {}
    current: Optional[AGSTable] = None
    last_legacy_row: Optional[Dict[str, str]] = None
    last_legacy_row_width = 0

    for raw_row in _read_csv_rows(_sanitize_ags_text(text)):
        first_raw = raw_row[0].strip()
        first = _normalize_row_label(first_raw)
        if _is_legacy_group_row(first_raw):
            group = _normalize_code(first_raw[2:])
            current = AGSTable(group=group, headings=[], layout="ags3")
            tables[group] = current
            last_legacy_row = None
            last_legacy_row_width = 0
            continue
        if first == "GROUP" and len(raw_row) > 1:
            group = _normalize_code(raw_row[1])
            current = AGSTable(group=group, headings=[], layout="ags4")
            tables[group] = current
            last_legacy_row = None
            last_legacy_row_width = 0
            continue
        if current is None:
            continue
        if current.layout == "ags4":
            if first == "HEADING":
                current.headings = [_normalize_code(value) for value in raw_row[1:]]
                continue
            if first in {"UNIT", "UNITS"}:
                current.units = raw_row[1:]
                current.source_units = list(current.units)
                continue
            if first == "TYPE":
                current.types = raw_row[1:]
                continue
            if first == "DATA":
                values = _align_values(raw_row[1:], len(current.headings))
                current.rows.append(dict(zip(current.headings, values)))
                continue
            if not current.headings:
                current.headings = [_normalize_code(value) for value in raw_row]
                continue
            values = _align_values(raw_row, len(current.headings))
            current.rows.append(dict(zip(current.headings, values)))
            continue

        if _is_legacy_heading_row(first_raw):
            headings = [_normalize_code(value) for value in _trim_trailing_empty_cells(raw_row) if _normalize_code(value)]
            if current.headings and not current.rows and not current.units and not current.types:
                current.headings.extend(headings)
            else:
                current.headings = headings
            last_legacy_row = None
            last_legacy_row_width = 0
            continue
        if _is_legacy_units_row(first_raw) or first in {"UNIT", "UNITS"}:
            current.units = raw_row[1:] if (_is_legacy_units_row(first_raw) or first in {"UNIT", "UNITS"}) else raw_row
            current.source_units = list(current.units)
            last_legacy_row = None
            last_legacy_row_width = 0
            continue
        if _is_legacy_continuation_row(first_raw):
            if last_legacy_row is not None:
                last_legacy_row_width = _merge_ags3_continuation_row(
                    existing=last_legacy_row,
                    headings=current.headings,
                    raw_row=raw_row,
                    previous_width=last_legacy_row_width,
                )
            continue
        if not current.headings:
            current.headings = [_normalize_code(value) for value in raw_row]
            continue
        values = _align_values(raw_row, len(current.headings))
        row = dict(zip(current.headings, values))
        current.rows.append(row)
        last_legacy_row = row
        last_legacy_row_width = min(len(raw_row), len(current.headings))

    return {group: table for group, table in tables.items() if table.headings}


def _resolved_source_units(table: AGSTable) -> List[str]:
    raw_units = list(table.source_units or table.units)
    if table.layout == "ags3" and _ags3_units_applicable(table.group) and len(raw_units) == len(table.headings) - 1:
        return ["", *raw_units]
    return _align_values(raw_units, len(table.headings))


def _backfill_table_units_from_references(parsed_tables: Dict[str, AGSTable], references: SchemaReferences) -> None:
    for table in parsed_tables.values():
        if not table.headings:
            continue

        if table.layout == "ags3":
            raw_units = _resolved_source_units(table)
            resolved_units: List[str] = []
            for heading, unit in zip(table.headings, raw_units):
                resolved_units.append(unit or references.ags3_headings.get(table.group, {}).get(heading, {}).get("unit", ""))
            table.units = resolved_units
            continue

        raw_units = _resolved_source_units(table)
        resolved_units: List[str] = []
        for heading, unit in zip(table.headings, raw_units):
            resolved_units.append(unit or references.ags4_headings.get(table.group, {}).get(heading, {}).get("unit", ""))
        table.units = resolved_units


def _ags3_heading_token(references: SchemaReferences, group: str, heading: str) -> str:
    meta = references.ags3_headings.get(group, {}).get(heading, {})
    code = meta.get("code", heading)
    is_optional = bool(meta.get("optional")) or str(code).startswith("?")
    display = _normalize_code(code)
    return f"*?{display}" if is_optional else f"*{display}"


def _ags3_group_token(references: SchemaReferences, group: str) -> str:
    meta = references.ags3_group_meta.get(group, {})
    code = str(meta.get("code", group))
    is_optional = bool(meta.get("optional")) or code.startswith("?")
    display = _normalize_code(code)
    return f"**?{display}" if is_optional else f"**{display}"


def _serialize_ags4_tables(tables: List[AGSTable], include_types: bool) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    for table in tables:
        writer.writerow(["GROUP", table.group])
        writer.writerow(["HEADING", *table.headings])
        writer.writerow(["UNIT", *table.units])
        if include_types:
            writer.writerow(["TYPE", *table.types])
        for row in table.rows:
            writer.writerow(["DATA", *[row.get(heading, "") for heading in table.headings]])
        writer.writerow([])
    return buffer.getvalue()


def _serialize_csv_row(cells: List[str]) -> str:
    buffer = StringIO()
    csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="").writerow(cells)
    return buffer.getvalue()


def _csv_row_length(cells: List[str]) -> int:
    return len(_serialize_csv_row(cells))


def _csv_row_length_with_padding(cells: List[str], total_columns: int) -> int:
    if len(cells) > total_columns:
        raise ValueError("cells exceed total_columns")
    return _csv_row_length(cells + ([""] * (total_columns - len(cells))))


def _largest_fitting_padded_prefix(base_cells: List[str], value: str, total_columns: int, max_length: int) -> int:
    if value == "":
        return 0
    low = 1
    high = len(value)
    best = 0
    while low <= high:
        middle = (low + high) // 2
        candidate = base_cells + [value[:middle]]
        if _csv_row_length_with_padding(candidate, total_columns) <= max_length:
            best = middle
            low = middle + 1
        else:
            high = middle - 1
    return best


def _wrap_ags3_token_row(cells: List[str], max_length: int = 240) -> List[Tuple[List[str], bool]]:
    wrapped_rows: List[Tuple[List[str], bool]] = []
    current_row: List[str] = []
    for index, cell in enumerate(cells):
        candidate = current_row + [cell]
        candidate_length = _csv_row_length(candidate) + (1 if index < len(cells) - 1 else 0)
        if current_row and candidate_length > max_length:
            wrapped_rows.append((current_row, True))
            current_row = [cell]
            continue
        current_row = candidate
    if current_row:
        wrapped_rows.append((current_row, False))
    return wrapped_rows


def _wrap_ags3_data_row(cells: List[str], max_length: int = 240) -> List[List[str]]:
    wrapped_rows: List[List[str]] = []
    total_columns = len(cells)
    column_index = 0
    remainder: Optional[str] = None
    first_line = True

    while column_index < total_columns:
        if first_line:
            line: List[str] = []
        else:
            if column_index <= 0:
                raise ValueError("Cannot continue AGS3 row inside the first data variable")
            line = ["<CONT>", *([""] * (column_index - 1))]
        emitted_any = False

        while column_index < total_columns:
            fragment = remainder if remainder is not None else cells[column_index]
            candidate = line + [fragment]
            if _csv_row_length_with_padding(candidate, total_columns) <= max_length:
                line.append(fragment)
                emitted_any = True
                remainder = None
                column_index += 1
                continue

            prefix_length = _largest_fitting_padded_prefix(line, fragment, total_columns, max_length)
            if prefix_length > 0:
                fragment_prefix = fragment[:prefix_length]
                line.append(fragment_prefix)
                emitted_any = True
                remainder = fragment[len(fragment_prefix):]
                if remainder == "":
                    remainder = None
                    column_index += 1
                break

            if emitted_any:
                break

            if prefix_length <= 0:
                raise ValueError("Unable to wrap AGS3 row within 240 character limit")

        if not emitted_any:
            raise ValueError("Unable to wrap AGS3 row within 240 character limit")

        wrapped_rows.append(line + ([""] * (total_columns - len(line))))
        first_line = False

    return wrapped_rows


_AGS3_GROUPS_WITHOUT_UNITS = {"ABBR", "CODE", "DICT", "UNIT"}


def _ags3_units_applicable(group: str) -> bool:
    return group not in _AGS3_GROUPS_WITHOUT_UNITS


def _serialize_ags3_tables(tables: List[AGSTable], references: SchemaReferences) -> str:
    lines: List[str] = []
    for table in tables:
        lines.append(_serialize_csv_row([_ags3_group_token(references, table.group)]))
        for heading_row, continued in _wrap_ags3_token_row(
            [_ags3_heading_token(references, table.group, heading) for heading in table.headings]
        ):
            lines.append(_serialize_csv_row(heading_row) + ("," if continued else ""))
        if _ags3_units_applicable(table.group):
            for units_row, continued in _wrap_ags3_token_row(["<UNITS>", *table.units[1:]]):
                lines.append(_serialize_csv_row(units_row) + ("," if continued else ""))
        for row in table.rows:
            for value_row in _wrap_ags3_data_row([row.get(heading, "") for heading in table.headings]):
                lines.append(_serialize_csv_row(value_row))
        lines.append("")
    return "\n".join(lines) + "\n"


def _default_output_path(input_ags_path: str, suffix: str) -> str:
    return str(Path(input_ags_path).with_suffix("")) + suffix


def _validate_paths(input_ags_path: str, output_ags_path: Optional[str]) -> str:
    if not isinstance(input_ags_path, str):
        raise TypeError("input_ags_path must be a string")
    if not input_ags_path.lower().endswith(".ags"):
        raise ValueError("input_ags_path must be a .ags file")
    if output_ags_path is not None:
        if not isinstance(output_ags_path, str) or not output_ags_path.lower().endswith(".ags"):
            raise TypeError("output_ags_path must be None, or a string ending with .ags")
        return output_ags_path
    return ""


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _write_text(path: str, contents: str) -> None:
    with Path(path).open("w", encoding="utf-8", newline="") as handle:
        handle.write(contents)


def _transform_date_value(value: str, source_meta: Optional[Dict[str, str]], target_meta: Optional[Dict[str, str]]) -> str:
    value = value.strip()
    if not value or source_meta is None or target_meta is None:
        return value

    source_type = source_meta.get("type", source_meta.get("dataType", ""))
    target_type = target_meta.get("type", target_meta.get("dataType", ""))
    if source_type == "Date" and target_type == "DT":
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    if source_type == "DT" and target_type == "Date":
        date_portion = value.split("T", 1)[0]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_portion, fmt).strftime("%d/%m/%Y")
            except ValueError:
                continue
    return value


def _merge_value(record: RowRecord, heading: str, value: str, conflict_prefix: str) -> None:
    if value == "":
        return
    if heading in record.conflicts:
        return
    existing = record.values.get(heading, "")
    if existing in {"", value}:
        record.values[heading] = value if value else existing
        return
    record.values[heading] = ""
    record.conflicts.add(heading)
    logger.warning("%s conflict for %s: %r vs %r", conflict_prefix, heading, existing, value)


def _combine_ags3_date_time(date_value: str, time_value: str) -> str:
    date_value = date_value.strip()
    time_value = time_value.strip()
    if not date_value and not time_value:
        return ""

    iso_date = ""
    if date_value:
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                iso_date = datetime.strptime(date_value, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        if not iso_date:
            iso_date = date_value

    normalized_time = ""
    if time_value:
        digits_only = re.sub(r"[^0-9]", "", time_value)
        if len(digits_only) >= 4:
            normalized_time = f"{digits_only[:2]}:{digits_only[2:4]}"
        else:
            normalized_time = time_value

    if iso_date and normalized_time:
        return f"{iso_date}T{normalized_time}"
    return iso_date or normalized_time


def _split_ags4_datetime(value: str) -> Tuple[str, str]:
    value = value.strip()
    if not value:
        return "", ""

    date_part, time_part = value, ""
    if "T" in value:
        date_part, time_part = value.split("T", 1)

    output_date = ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            output_date = datetime.strptime(date_part, fmt).strftime("%d/%m/%Y")
            break
        except ValueError:
            continue
    if not output_date:
        output_date = date_part

    output_time = ""
    if time_part:
        digits_only = re.sub(r"[^0-9]", "", time_part)
        if len(digits_only) >= 6:
            output_time = digits_only[:6]
        elif len(digits_only) >= 4:
            output_time = digits_only[:4]
        else:
            output_time = digits_only

    return output_date, output_time


def _normalize_numeric_precision_value(value: str, data_type: str) -> str:
    stripped = value.strip()
    if not stripped:
        return stripped

    dp_match = re.fullmatch(r"(\d+)DP", data_type.strip().upper())
    sf_match = re.fullmatch(r"(\d+)SF", data_type.strip().upper())
    if not dp_match and not sf_match:
        return stripped

    number_match = re.fullmatch(r"([+-]?)(\d+)(?:\.(\d*))?", stripped)
    if not number_match:
        return stripped

    sign, whole, fraction = number_match.groups()
    fraction = fraction or ""

    if dp_match:
        required_dp = int(dp_match.group(1))
        if len(fraction) > required_dp:
            return stripped
        if required_dp == 0:
            return f"{sign}{whole}" if not fraction else stripped
        return f"{sign}{whole}.{fraction.ljust(required_dp, '0')}"

    required_sf = int(sf_match.group(1))
    significant_digits = (whole + fraction).lstrip("0")
    current_sf = len(significant_digits)
    if current_sf >= required_sf:
        return stripped
    if current_sf == 0:
        return f"{sign}0" if required_sf == 1 else f"{sign}0.{('0' * (required_sf - 1))}"
    if fraction or whole == "0":
        return f"{sign}{whole}.{fraction}{('0' * (required_sf - current_sf))}"
    return f"{sign}{whole}.{('0' * (required_sf - current_sf))}"


def _heading_source_units(table: AGSTable) -> Dict[str, str]:
    return dict(zip(table.headings, _resolved_source_units(table)))


def _record_source_unit_candidate(
    unit_candidates: Dict[str, Dict[str, set[str]]], group: str, heading: str, unit: str
) -> None:
    normalized = unit.strip()
    if not normalized:
        return
    unit_candidates.setdefault(group, {}).setdefault(heading, set()).add(normalized)


def _record_upgrade_synthetic_unit_candidates(
    unit_candidates: Dict[str, Dict[str, set[str]]], source_units: Dict[str, str], row: Dict[str, str]
) -> None:
    if not row.get("HOLE_ID", ""):
        return
    synthetic_mappings = {
        "HDPH_TOP": source_units.get("HOLE_FDEP", ""),
        "HDPH_BASE": source_units.get("HOLE_FDEP", ""),
        "HDPH_TYPE": source_units.get("HOLE_TYPE", ""),
        "HDPH_STAR": source_units.get("HOLE_STAR", ""),
        "HDPH_ENDD": source_units.get("HOLE_ENDD", ""),
    }
    for heading, unit in synthetic_mappings.items():
        _record_source_unit_candidate(unit_candidates, "HDPH", heading, unit)


def _detect_date_shape(value: str) -> Optional[TemporalUnitShape]:
    stripped = value.strip()
    if re.fullmatch(r"\d{4}", stripped):
        return TemporalUnitShape(family="date", style="iso", precision=1)
    if re.fullmatch(r"\d{4}-\d{2}", stripped):
        return TemporalUnitShape(family="date", style="iso", precision=2)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", stripped):
        return TemporalUnitShape(family="date", style="iso", precision=3)
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", stripped):
        return TemporalUnitShape(family="date", style="legacy", precision=3)
    return None


def _detect_time_shape(value: str) -> Optional[TemporalUnitShape]:
    stripped = value.strip()
    if not stripped:
        return None

    timezone = False
    if stripped.endswith("Z"):
        timezone = True
        stripped = stripped[:-1]
    else:
        offset_match = re.search(r"([+-]\d{2}:\d{2})$", stripped)
        if offset_match:
            timezone = True
            stripped = stripped[: -len(offset_match.group(1))]

    if re.fullmatch(r"\d{4}", stripped):
        return TemporalUnitShape(family="time", style="compact", precision=2, has_timezone=timezone)
    if re.fullmatch(r"\d{6}", stripped):
        return TemporalUnitShape(family="time", style="compact", precision=3, has_timezone=timezone)
    if re.fullmatch(r"\d{6}\.\d+", stripped):
        return TemporalUnitShape(
            family="time", style="compact", precision=4, has_fraction=True, has_timezone=timezone
        )
    if re.fullmatch(r"\d{2}:\d{2}", stripped):
        return TemporalUnitShape(family="time", style="colon", precision=2, has_timezone=timezone)
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", stripped):
        return TemporalUnitShape(family="time", style="colon", precision=3, has_timezone=timezone)
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}\.\d+", stripped):
        return TemporalUnitShape(family="time", style="colon", precision=4, has_fraction=True, has_timezone=timezone)
    return None


def _format_time_shape(shape: TemporalUnitShape) -> str:
    if shape.style == "compact":
        base = "hhmm"
        if shape.precision >= 3:
            base += "ss"
        if shape.has_fraction:
            base += ".sss"
    else:
        base = "hh:mm"
        if shape.precision >= 3:
            base += ":ss"
        if shape.has_fraction:
            base += ".sss"
    if shape.has_timezone:
        base += "Z(+hh:mm)"
    return base


def _merge_shapes(shapes: List[TemporalUnitShape]) -> Optional[TemporalUnitShape]:
    if not shapes:
        return None
    styles = {shape.style for shape in shapes}
    families = {shape.family for shape in shapes}
    if len(styles) != 1 or len(families) != 1:
        return None
    return TemporalUnitShape(
        family=shapes[0].family,
        style=shapes[0].style,
        precision=max(shape.precision for shape in shapes),
        has_fraction=any(shape.has_fraction for shape in shapes),
        has_timezone=any(shape.has_timezone for shape in shapes),
    )


def _infer_temporal_unit(values: List[str], meta: Dict[str, str]) -> str:
    temporal_type = meta.get("dataType", meta.get("type", ""))
    observed = [value.strip() for value in values if value.strip()]
    if not observed:
        return ""

    if temporal_type == "Date":
        shapes = [_detect_date_shape(value) for value in observed]
        if any(shape is None for shape in shapes):
            return ""
        merged = _merge_shapes([shape for shape in shapes if shape is not None])
        if merged is None:
            return ""
        if merged.style == "legacy":
            return "dd/mm/yyyy"
        if merged.precision == 1:
            return "yyyy"
        if merged.precision == 2:
            return "yyyy-mm"
        return "yyyy-mm-dd"

    if temporal_type == "Time":
        shapes = [_detect_time_shape(value) for value in observed]
        if any(shape is None for shape in shapes):
            return ""
        merged = _merge_shapes([shape for shape in shapes if shape is not None])
        return _format_time_shape(merged) if merged else ""

    if temporal_type != "DT":
        return ""

    date_shapes: List[TemporalUnitShape] = []
    time_shapes: List[TemporalUnitShape] = []
    has_datetime = False
    for value in observed:
        if "T" in value:
            date_part, time_part = value.split("T", 1)
            date_shape = _detect_date_shape(date_part)
            time_shape = _detect_time_shape(time_part)
            if date_shape is None or time_shape is None:
                return ""
            has_datetime = True
            date_shapes.append(date_shape)
            time_shapes.append(time_shape)
            continue

        date_shape = _detect_date_shape(value)
        if date_shape is not None:
            date_shapes.append(date_shape)
            continue

        time_shape = _detect_time_shape(value)
        if time_shape is not None:
            time_shapes.append(time_shape)
            continue

        return ""

    if date_shapes and time_shapes:
        merged_date = _merge_shapes(date_shapes)
        merged_time = _merge_shapes(time_shapes)
        if merged_date is None or merged_time is None:
            return ""
        if merged_date.style != "iso":
            return ""
        date_unit = "yyyy" if merged_date.precision == 1 else "yyyy-mm" if merged_date.precision == 2 else "yyyy-mm-dd"
        return f"{date_unit}T{_format_time_shape(merged_time)}"

    if has_datetime:
        return ""
    if date_shapes:
        merged_date = _merge_shapes(date_shapes)
        if merged_date is None:
            return ""
        if merged_date.style == "legacy":
            return "dd/mm/yyyy"
        if merged_date.precision == 1:
            return "yyyy"
        if merged_date.precision == 2:
            return "yyyy-mm"
        return "yyyy-mm-dd"
    if time_shapes:
        merged_time = _merge_shapes(time_shapes)
        return _format_time_shape(merged_time) if merged_time else ""
    return ""


def _resolve_output_unit(
    group: str,
    heading: str,
    meta: Dict[str, str],
    values: List[str],
    unit_candidates: Dict[str, Dict[str, set[str]]],
) -> str:
    inferred_temporal = _infer_temporal_unit(values, meta)
    if inferred_temporal:
        return inferred_temporal

    source_candidates = unit_candidates.get(group, {}).get(heading, set())
    if len(source_candidates) == 1:
        return next(iter(source_candidates))
    if len(source_candidates) > 1:
        logger.warning("Unit conflict for %s.%s: %s; falling back to schema reference", group, heading, sorted(source_candidates))

    return meta.get("unit", "")


def _find_matching_record(records: List[RowRecord], key_headings: List[str], fragment: Dict[str, str]) -> Optional[RowRecord]:
    if not records:
        return None

    if not key_headings:
        return None

    non_blank_keys = {heading: fragment.get(heading, "") for heading in key_headings if fragment.get(heading, "")}
    if key_headings:
        exact_matches = [
            record
            for record in records
            if all(record.values.get(heading, "") == fragment.get(heading, "") for heading in key_headings)
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]

    if non_blank_keys:
        partial_matches = [
            record
            for record in records
            if all(record.values.get(heading, "") == value for heading, value in non_blank_keys.items())
        ]
        if len(partial_matches) == 1:
            return partial_matches[0]

    if not non_blank_keys and len(records) == 1:
        return records[0]

    return None


def _group_has_payload(group: str, fragment: Dict[str, str], key_headings: List[str]) -> bool:
    if group == "HDPH":
        return any(fragment.get(heading, "") for heading in fragment if heading != "LOCA_ID")
    return any(fragment.get(heading, "") for heading in fragment if heading not in key_headings) or any(
        fragment.get(heading, "") for heading in key_headings
    )


def _aggregate_fragment(
    grouped_rows: Dict[str, List[RowRecord]],
    target_group: str,
    fragment: Dict[str, str],
    key_headings: List[str],
    conflict_prefix: str,
    fragment_conflicts: Optional[set[str]] = None,
) -> None:
    if not _group_has_payload(target_group, fragment, key_headings):
        return

    records = grouped_rows.setdefault(target_group, [])
    match = _find_matching_record(records, key_headings, fragment)
    has_any_key = any(fragment.get(heading, "") for heading in key_headings)
    if match is None and key_headings and not has_any_key:
        return
    if match is None:
        match = RowRecord(values={heading: fragment.get(heading, "") for heading in key_headings if fragment.get(heading, "")})
        records.append(match)

    for heading in fragment_conflicts or set():
        match.values.setdefault(heading, "")
        match.conflicts.add(heading)
    for heading in key_headings:
        if heading in fragment and heading not in match.values:
            match.values[heading] = fragment[heading]
    for heading, value in fragment.items():
        _merge_value(match, heading, value, conflict_prefix)


def _ags3_heading_meta(references: SchemaReferences, group: str, heading: str) -> Optional[Dict[str, str]]:
    return references.ags3_headings.get(group, {}).get(heading)


def _ags4_heading_meta(references: SchemaReferences, group: str, heading: str) -> Optional[Dict[str, str]]:
    return references.ags4_headings.get(group, {}).get(heading)


def _apply_upgrade_synthetic_logic(row: Dict[str, str], fragments: Dict[str, RowRecord]) -> None:
    hole_id = row.get("HOLE_ID", "")
    hole_depth = row.get("HOLE_FDEP", "")
    loca_values = fragments.get("LOCA", RowRecord()).values
    hole_type = loca_values.get("LOCA_TYPE", row.get("HOLE_TYPE", ""))
    hole_start = loca_values.get("LOCA_STAR", row.get("HOLE_STAR", ""))
    hole_end = loca_values.get("LOCA_ENDD", row.get("HOLE_ENDD", ""))
    if not hole_id:
        return

    hdph = fragments.setdefault("HDPH", RowRecord())
    hdph.values.setdefault("LOCA_ID", hole_id)
    if hole_depth:
        hdph.values.setdefault("HDPH_TOP", "0.00")
        hdph.values.setdefault("HDPH_BASE", hole_depth)
    if hole_type:
        hdph.values.setdefault("HDPH_TYPE", hole_type)
    if hole_start:
        hdph.values.setdefault("HDPH_STAR", hole_start)
    if hole_end:
        hdph.values.setdefault("HDPH_ENDD", hole_end)


def _apply_upgrade_target_overrides(grouped_rows: Dict[str, List[RowRecord]], ags4_version: str) -> None:
    for record in grouped_rows.get("TRAN", []):
        record.values["TRAN_AGS"] = ags4_version


def _apply_downgrade_target_overrides(grouped_rows: Dict[str, List[RowRecord]]) -> None:
    for record in grouped_rows.get("PROJ", []):
        record.values["PROJ_AGS"] = "3.1"


def _build_upgrade_tables(parsed_tables: Dict[str, AGSTable], references: SchemaReferences) -> List[AGSTable]:
    grouped_rows: Dict[str, List[RowRecord]] = {}
    unit_candidates: Dict[str, Dict[str, set[str]]] = {}

    for source_group, table in parsed_tables.items():
        source_units = _heading_source_units(table)
        for row in table.rows:
            fragments: Dict[str, RowRecord] = {}
            for source_heading, raw_value in row.items():
                if source_group == "PTIM" and source_heading in {"PTIM_DATE", "PTIM_TIME"}:
                    continue
                mappings = references.forward_crosswalk.get((source_group, source_heading), [])
                if not mappings:
                    continue
                source_meta = _ags3_heading_meta(references, source_group, source_heading)
                for target_group, target_heading in mappings:
                    target_meta = _ags4_heading_meta(references, target_group, target_heading)
                    value = _transform_date_value(raw_value, source_meta, target_meta)
                    fragment_record = fragments.setdefault(target_group, RowRecord())
                    _merge_value(fragment_record, target_heading, value, f"Upgrade {source_group}->{target_group}")
                    _record_source_unit_candidate(
                        unit_candidates, target_group, target_heading, source_units.get(source_heading, "")
                    )

            if source_group == "PTIM":
                combined = _combine_ags3_date_time(row.get("PTIM_DATE", ""), row.get("PTIM_TIME", ""))
                if combined:
                    fragment_record = fragments.setdefault("PTIM", RowRecord())
                    fragment_record.values["PTIM_DTIM"] = combined

            if source_group == "HOLE":
                _apply_upgrade_synthetic_logic(row, fragments)
                _record_upgrade_synthetic_unit_candidates(unit_candidates, source_units, row)

            for target_group, fragment_record in fragments.items():
                key_headings = references.ags4_keys.get(target_group, [])
                _aggregate_fragment(
                    grouped_rows=grouped_rows,
                    target_group=target_group,
                    fragment=fragment_record.values,
                    key_headings=key_headings,
                    conflict_prefix=f"Upgrade {source_group}->{target_group}",
                    fragment_conflicts=fragment_record.conflicts,
                )

    _apply_upgrade_target_overrides(grouped_rows, references.ags4_version)

    output_tables: List[AGSTable] = []
    for group in references.ags4_groups:
        records = grouped_rows.get(group, [])
        if not records:
            continue
        heading_meta = references.ags4_headings.get(group, {})
        key_headings = references.ags4_keys.get(group, [])
        used_headings = set(key_headings)
        for record in records:
            used_headings.update(heading for heading, value in record.values.items() if value or heading in record.conflicts)
        ordered_headings = [heading for heading in heading_meta.keys() if heading in used_headings]
        if not ordered_headings:
            continue
        types = [heading_meta[heading].get("dataType", "X") for heading in ordered_headings]
        rows = []
        for record in records:
            rows.append(
                {
                    heading: _normalize_numeric_precision_value(
                        record.values.get(heading, ""),
                        heading_meta[heading].get("dataType", "X"),
                    )
                    for heading in ordered_headings
                }
            )
        units = [
            _resolve_output_unit(
                group=group,
                heading=heading,
                meta=heading_meta[heading],
                values=[row.get(heading, "") for row in rows],
                unit_candidates=unit_candidates,
            )
            for heading in ordered_headings
        ]
        output_tables.append(AGSTable(group=group, headings=ordered_headings, units=units, types=types, rows=rows))

    return output_tables


def _build_downgrade_tables(parsed_tables: Dict[str, AGSTable], references: SchemaReferences) -> List[AGSTable]:
    grouped_rows: Dict[str, List[RowRecord]] = {}
    deferred_fragments: List[Tuple[str, RowRecord, str]] = []
    unit_candidates: Dict[str, Dict[str, set[str]]] = {}

    for target_group, table in parsed_tables.items():
        source_units = _heading_source_units(table)
        for row in table.rows:
            fragments: Dict[str, RowRecord] = {}
            for target_heading, raw_value in row.items():
                if target_group == "PTIM" and target_heading == "PTIM_DTIM":
                    continue
                mappings = references.reverse_crosswalk.get((target_group, target_heading), [])
                if not mappings:
                    continue
                source_meta = _ags4_heading_meta(references, target_group, target_heading)
                for source_group, source_heading in mappings:
                    target_meta = _ags3_heading_meta(references, source_group, source_heading)
                    value = _transform_date_value(raw_value, source_meta, target_meta)
                    fragment_record = fragments.setdefault(source_group, RowRecord())
                    _merge_value(fragment_record, source_heading, value, f"Downgrade {target_group}->{source_group}")
                    _record_source_unit_candidate(
                        unit_candidates, source_group, source_heading, source_units.get(target_heading, "")
                    )

            if target_group == "PTIM" and row.get("PTIM_DTIM", "").strip():
                output_date, output_time = _split_ags4_datetime(row.get("PTIM_DTIM", ""))
                fragment_record = fragments.setdefault("PTIM", RowRecord())
                if output_date:
                    fragment_record.values["PTIM_DATE"] = output_date
                if output_time:
                    fragment_record.values["PTIM_TIME"] = output_time

            for source_group, fragment_record in fragments.items():
                key_headings = references.ags3_keys.get(source_group, [])
                if key_headings and not any(fragment_record.values.get(heading, "") for heading in key_headings):
                    deferred_fragments.append((source_group, fragment_record, target_group))
                    continue
                _aggregate_fragment(
                    grouped_rows=grouped_rows,
                    target_group=source_group,
                    fragment=fragment_record.values,
                    key_headings=key_headings,
                    conflict_prefix=f"Downgrade {target_group}->{source_group}",
                    fragment_conflicts=fragment_record.conflicts,
                )

    for source_group, fragment_record, target_group in deferred_fragments:
        _aggregate_fragment(
            grouped_rows=grouped_rows,
            target_group=source_group,
            fragment=fragment_record.values,
            key_headings=references.ags3_keys.get(source_group, []),
            conflict_prefix=f"Downgrade {target_group}->{source_group}",
            fragment_conflicts=fragment_record.conflicts,
        )

    _apply_downgrade_target_overrides(grouped_rows)

    output_tables: List[AGSTable] = []
    for group in references.ags3_groups:
        records = grouped_rows.get(group, [])
        if not records:
            continue
        heading_meta = references.ags3_headings.get(group, {})
        key_headings = references.ags3_keys.get(group, [])
        used_headings = set(key_headings)
        for record in records:
            used_headings.update(heading for heading, value in record.values.items() if value or heading in record.conflicts)
        ordered_headings = [heading for heading in heading_meta.keys() if heading in used_headings]
        if not ordered_headings:
            continue
        rows = []
        for record in records:
            rows.append({heading: record.values.get(heading, "") for heading in ordered_headings})
        units = [
            _resolve_output_unit(
                group=group,
                heading=heading,
                meta=heading_meta[heading],
                values=[row.get(heading, "") for row in rows],
                unit_candidates=unit_candidates,
            )
            for heading in ordered_headings
        ]
        output_tables.append(AGSTable(group=group, headings=ordered_headings, units=units, rows=rows))

    return output_tables


def upgrade(input_ags_path: str, output_ags_path: Optional[str] = None, version: Optional[str] = None) -> None:
    validated_output = _validate_paths(input_ags_path, output_ags_path)
    output_path = validated_output or _default_output_path(input_ags_path, "_AGS4.ags")
    references = _load_schema_references(version)
    parsed_tables = _parse_ags_tables(_read_text(input_ags_path))
    _backfill_table_units_from_references(parsed_tables, references)
    output_tables = _build_upgrade_tables(parsed_tables, references)
    _write_text(output_path, _serialize_ags4_tables(output_tables, include_types=True))


def downgrade(input_ags_path: str, output_ags_path: Optional[str] = None) -> None:
    validated_output = _validate_paths(input_ags_path, output_ags_path)
    output_path = validated_output or _default_output_path(input_ags_path, "_AGS3.ags")
    references = _load_schema_references()
    parsed_tables = _parse_ags_tables(_read_text(input_ags_path))
    _backfill_table_units_from_references(parsed_tables, references)
    output_tables = _build_downgrade_tables(parsed_tables, references)
    _write_text(output_path, _serialize_ags3_tables(output_tables, references))
