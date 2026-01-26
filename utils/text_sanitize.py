import html, re

_HIDDEN_PATTERNS = [
    r"\(\s*link opens a new window\s*\)$",
    r"\(\s*opens in new window\s*\)$",
]

def clean_company_name(name: str | None) -> str:
    if not name:
        return ""
    s = html.unescape(str(name)).strip()
    for pat in _HIDDEN_PATTERNS:
        s = re.sub(pat, "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s+", " ", s)  # collapse whitespace
    return s
    