# taxonomy.py

from dataclasses import dataclass
from typing import List, Optional, Dict, Callable, Pattern
import re

@dataclass
class ProcedureDef:
    key: str                      # stable key used in code
    label: str                    # client-facing label
    folder: str                   # folder name convention
    gazette_codes: List[str]      # categorycode=... values
    enabled_in_gazette: bool      # shown in Search by Procedure tab
    notice_filters: Optional[List[Pattern]] = None  # optional regex filters on notice_type

# Gazette category codes you already use
GZ = {
    "administration": "G305010100",
    "cvl": "G305010200",
    "liquidation_by_court": "G305010300",
    "mvl": "G305010400",
    "receivership": "G305010500",
    # (the ones we are hiding are intentionally not referenced)
}

# Notice type filter: Admin Receivers only
ADMIN_RECEIVER_PATTERNS = [
    re.compile(r"appointment of administrative receivers", re.IGNORECASE),
]

PROCEDURES: List[ProcedureDef] = [
    # Gazette-backed (show in Gazette tab)
    ProcedureDef(
        key="administration",
        label="Administration",
        folder="Administration",
        gazette_codes=[GZ["administration"]],
        enabled_in_gazette=True
    ),
    ProcedureDef(
        key="administrative_receivership",
        label="Administrative Receivership",
        folder="Administrative Receivership",
        gazette_codes=[GZ["receivership"]],
        enabled_in_gazette=True,
        notice_filters=ADMIN_RECEIVER_PATTERNS
    ),
    ProcedureDef(
        key="mvl",
        label="Members’ Voluntary Winding Up",
        folder="Members’ Voluntary Winding Up",
        gazette_codes=[GZ["mvl"]],
        enabled_in_gazette=True
    ),
    ProcedureDef(
        key="cvl",
        label="Creditors’ Voluntary Winding Up",
        folder="Creditors’ Voluntary Winding Up",
        gazette_codes=[GZ["cvl"]],
        enabled_in_gazette=True
    ),
    ProcedureDef(
        key="court_wu",
        label="Compulsory Liquidation/Winding up by the Court",
        folder="Compulsory Liquidation/Winding up by the Court",
        gazette_codes=[GZ["liquidation_by_court"]],
        enabled_in_gazette=True
    ),

    # Separate tabs (not Gazette)
    ProcedureDef(
        key="moratorium",
        label="Moratorium",
        folder="Moratorium",
        gazette_codes=[],               # not used here
        enabled_in_gazette=False
    ),
    ProcedureDef(
        key="cva",
        label="Company Voluntary Arrangements",
        folder="CVA",
        gazette_codes=[],               # not used here
        enabled_in_gazette=False
    ),
]

# For backward compatibility (reading old cache/classifiers)
ALIASES: Dict[str, str] = {
    # old → new folder or canonical key
    "CVL": "Creditors’ Voluntary Winding Up",
    "CVWU": "Creditors’ Voluntary Winding Up",
    "MVL": "Members’ Voluntary Winding Up",
    "MVWU": "Members’ Voluntary Winding Up",
    "liquidation_by_court": "Compulsory Liquidation/Winding up by the Court",
    "court_liquidation": "Compulsory Liquidation/Winding up by the Court",
    "Court_Liquidation": "Compulsory Liquidation/Winding up by the Court",
    "receivership": "Administrative Receivership",   # when filtered as admin receivers
    "Administrative_Receivership": "Administrative Receivership",
}
