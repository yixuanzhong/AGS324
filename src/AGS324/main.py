# src/AGS324/main.py

from typing import List, Optional, Union
from traceback import format_exc
import logging

# Set up logging
logger = logging.getLogger(__name__)

def _find_all_indices(string: str, substring: str) -> List:
    """Return a list of starting indices of all occurrence of a substring in a string"""
    return [i for i in range(len(string)) if string.startswith(substring, i)]

def _replace_char(text: str, at_index: int, new_char: str, length: int=1) -> str:
    """Return the modified text, with new_char replacing length number of characters at at_index"""
    return text[:at_index] + new_char + text[at_index+length:]

def ags4_c3dgm(input_ags_path: str, output_ags_path: str=None) -> None:
    """Return reformatted .ags file in AGS4 schema good for C3DGM only, from input .ags file path"""
    try:
        if not isinstance(input_ags_path, str):
            raise TypeError("input_ags_path must be a string")
        if not input_ags_path.endswith('.ags'):
            raise ValueError("input_ags_path must be a .ags file")
        if output_ags_path is not None and not isinstance(output_ags_path, str) and not output_ags_path.endswith('.ags'):
            raise TypeError("output_ags_path must be None, or a string ending with .ags")

        if output_ags_path is None:
            output_ags_path = input_ags_path[:-4] + "_AGS4.ags"
    
        import json
        import re
        import os
        from io import StringIO
        import pandas as pd

        current_dir = os.path.dirname(os.path.abspath(__file__))
        ags4_standard_headers = os.path.join(current_dir, "ags4_standard_headers.json")

        with open(ags4_standard_headers) as f:
            standards = json.load(f)
        with open(input_ags_path, 'r') as f:
            contents = re.sub(r'[","]*("\n"<CONT>)[","]*', "<C>", f.read()) # Match <CONT> and replace with <C>

        # Text parsing and cleaning
        contents = contents.replace('?', '').replace('"<UNITS>"', '"UNITS",""')
        contents = re.sub(r'^"GROUP".*\n?', '', contents, flags=re.MULTILINE)

        original_table_names = re.findall(r'\"\*\*[a-zA-Z0-9]+\"', contents)
        table_names = [name[3:-1] for name in original_table_names] # Remove **


        table_start_indices = {table_name:contents.find('"**' + table_name) for table_name in table_names}
        table_end_indices = {table_name:index for table_name, index in zip(table_names, list(table_start_indices.values())[1:]+[len(contents)])}

        new_contents = ""
        new_table_names = ['PROJ', 'ABBR', 'DICT', 'UNIT', 'GEOL', 'ISPT', 'HOLE', 'HDPH'] # Minimally required tables for processing
        for table_name in new_table_names:
            try:
                new_contents += contents[table_start_indices[table_name]:table_end_indices[table_name]]
            except Exception as e:
                continue

        new_contents = new_contents.replace('"HEADING"', '"DATA", "HEADING"').replace('**', 'GROUP","')

        for name in new_table_names:
            check_index = new_contents.find('\n', new_contents.find('\n', new_contents.find('"GROUP","' + name)) + 1)
            if new_contents[check_index + 2] == '*':
                new_contents = _replace_char(new_contents, check_index, '')
        new_contents = new_contents.replace('*', '')

        for name in new_table_names:
            if new_contents.find('"GROUP","' + name) != -1:
                header_start_loc = new_contents.find('\n', new_contents.find('"GROUP","' + name)) + 1
                new_contents = _replace_char(new_contents, header_start_loc, '^') # Put '^' at place to insert "HEADING"
                header_end_loc = new_contents.find('\n', header_start_loc)

                headers = new_contents[header_start_loc:header_end_loc].replace('"', '').split(',')
                header_count = len(headers)
                type_row = '\n"TYPE"'
                for header in headers:
                    type_row = type_row + ',"' + standards.get(header, 'X') + '"'

                if new_contents[header_end_loc + 1 : header_end_loc + 8] == '"UNITS"':
                    insert_loc = new_contents.find('\n', header_end_loc + 1)
                    new_contents = _replace_char(new_contents, insert_loc, type_row + '\n')
                else:
                    insert_loc = header_end_loc
                    unit_row = '\n"UNITS"' + ',""' * header_count
                    new_contents = _replace_char(new_contents, insert_loc, unit_row + type_row + '\n')

        new_contents = new_contents.replace('^', '"HEADING","').replace('UNITS', 'UNIT')

        ags4_text = ""
        new_contents = new_contents.replace('HOLE', 'LOCA')
        for line in new_contents.split('\n'):
            if len(line) == 0 or line.startswith('"DATA"'):
                data_line = line + '\n'
            elif line[:6] in ('"GROUP', '"HEADI', '"UNIT"', '"TYPE"'):
                data_line = line + '\n'
            else:
                data_line = '"DATA",' + line + '\n'
            ags4_text += data_line
    
        if ags4_text.rfind('"HEADING","') + len('"HEADING","') == len(ags4_text) - 1:
            ags4_text = ags4_text[:ags4_text.rfind('"HEADING","')]

        # Find start and end of each table
        ags4_table_names = ['PROJ', 'ABBR', 'DICT', 'UNIT', 'GEOL', 'ISPT', 'LOCA'] # Minimally required tables for C3DGM

        range_start = []
        for name in ags4_table_names:
            range_start.append(ags4_text.find(f'"GROUP","{name}"'))
        range_end = range_start[1:] + [len(ags4_text)-1]

        range_dict = {}
        header_counts = {}
        for name, start, end in zip(ags4_table_names, range_start, range_end):
            range_dict[name] = range(start, end+1)
            header_counts[name] = len(ags4_text[ags4_text.find("HEADING", start): ags4_text.find("\n", start+15)].split('","'))

        # Remove <C>
        cont_indices = _find_all_indices(ags4_text, '<C>')
        cont_instances = re.findall(r'[^\n]+<C>[^\n]+', ags4_text)
        for cont_index, cont_instance in zip(cont_indices, cont_instances):
            for key, value in range_dict.items():
                if cont_index in value:
                    if header_counts[key] != len(cont_instance.split('","')):
                        ags4_text = _replace_char(ags4_text, cont_index, '","', length=3)
                    break
        ags4_text = ags4_text.replace("<C>", "")

        # Format HDPH table
        hdph_columns = ags4_text[ags4_text.find('"GROUP","LOCA"\n')+15:ags4_text.find('\n"UNIT"', ags4_text.find('"GROUP","LOCA"\n'))].replace('"','').split(',')
        hdph_contents = StringIO(ags4_text[ags4_text.find('\n"DATA"', ags4_text.find('"GROUP","LOCA"\n'))+1:])
        hdph_columns = ags4_text[ags4_text.find('"GROUP","LOCA"\n')+15:ags4_text.find('\n"UNIT"', ags4_text.find('"GROUP","LOCA"\n'))].replace('"','').split(',')
        hdph_contents = StringIO(ags4_text[ags4_text.find('\n"DATA"', ags4_text.find('"GROUP","LOCA"\n'))+1:])

        df_loca = pd.read_csv(hdph_contents)
        df_loca.columns = hdph_columns
        df_hdph = pd.DataFrame(columns = ["LOCA_ID","HDPH_TOP","HDPH_BASE","HDPH_TYPE","HDPH_STAR","HDPH_ENDD","HDPH_CREW","HDPH_EXC","HDPH_SHOR","HDPH_STAB","HDPH_DIML","HDPH_DIMW","HDPH_DBIT","HDPH_BCON","HDPH_BTYP","HDPH_BLEN","HDPH_LOG","HDPH_LOGD","HDPH_REM","HDPH_ENV","HDPH_METH","HDPH_CONT","FILE_FSET"])
        try:
            df_hdph['LOCA_ID'] = df_loca['LOCA_ID']
            df_hdph['HDPH_TOP'] = 0.00
            df_hdph['HDPH_BASE'] = df_loca['LOCA_FDEP']
            df_hdph['HDPH_TYPE'] = df_loca['LOCA_TYPE']
            df_hdph['HDPH_STAR'] = df_loca['LOCA_STAR']
            df_hdph['HDPH_ENDD'] = df_loca['LOCA_ENDD']
            df_hdph['HDPH_CREW'] = df_loca['LOCA_CREW']
            df_hdph['HDPH_EXC'] = df_loca['LOCA_EXC']
            df_hdph['HDPH_LOG'] = df_loca['LOCA_LOG']
            df_hdph['HDPH_REM'] = df_loca['LOCA_REM']
        except Exception as e:
            logger.warning(e)

        df_hdph = df_hdph.fillna("").astype(str).applymap(lambda x: x.replace("\r", ""))
        hdph_string = StringIO()
        df_hdph.to_csv(hdph_string, header=False, index=False, na_rep='', quoting=1, lineterminator='')

        hdph_headers = '"GROUP","HDPH"\n"HEADING","LOCA_ID","HDPH_TOP","HDPH_BASE","HDPH_TYPE","HDPH_STAR","HDPH_ENDD","HDPH_CREW","HDPH_EXC","HDPH_SHOR","HDPH_STAB","HDPH_DIML","HDPH_DIMW","HDPH_DBIT","HDPH_BCON","HDPH_BTYP","HDPH_BLEN","HDPH_LOG","HDPH_LOGD","HDPH_REM","HDPH_ENV","HDPH_METH","HDPH_CONT","FILE_FSET"\n"UNIT","","m","m","","yyyy-mm-ddThh:mm:ss","yyyy-mm-ddThh:mm:ss","","","","","m","m","","","","m","","yyyy-mm-dd","","","","",""\n"TYPE","ID","2DP","2DP","PA","DT","DT","X","X","X","X","2DP","2DP","X","X","X","2DP","X","DT","X","X","X","X","X"'
        hdph_table = hdph_string.getvalue().replace("\r\n", "\n")

        output_text = ags4_text + hdph_headers + "\n" + hdph_table + "\n"

        with open(output_ags_path, "w", encoding="utf-8") as f:
            f.write(output_text)

    except Exception as e:
        logger.error(format_exc())