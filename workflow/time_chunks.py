# workflow/time_chunks.py
import pandas as pd
from datetime import datetime


def infer_time_chunks(settings: dict) -> list[str]:
    """Used by Snakefile — derives chunks from product settings dict."""
    chunks = settings.get("time_chunks")
    if chunks:
        return chunks
    return get_time_chunks(
        settings["start_date"],
        settings["end_date"],
        settings.get("cadence", "monthly"),
    )


def get_time_chunks(start_str: str, end_str: str, cadence: str) -> list[str]:
    """Used by app.py — derives chunks from explicit start/end/cadence."""
    if cadence == "annual":
        return _year_list(start_str, end_str)
    if cadence == "daily":
        return _month_list(start_str, end_str)
    if cadence == "seasonal":
        return _seasonal_chunks(start_str, end_str)
    return _quarterly_chunks(start_str, end_str)


def chunk_start_date(time_chunk: str) -> str:
    if len(time_chunk) == 4:
        return f"{time_chunk}-01-01"
    return f"{time_chunk.split('_')[0]}-01"


def chunk_end_date(time_chunk: str) -> str:
    if len(time_chunk) == 4:
        return f"{time_chunk}-12-31"
    end = time_chunk.split("_")[-1]
    return (pd.to_datetime(end) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")


def _month_list(start_str: str, end_str: str) -> list[str]:
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end   = datetime.strptime(end_str,   "%Y-%m-%d")
    months, curr = [], start.replace(day=1)
    while curr <= end:
        months.append(curr.strftime("%Y-%m"))
        curr = (
            curr.replace(month=curr.month + 1)
            if curr.month < 12
            else curr.replace(year=curr.year + 1, month=1)
        )
    return months


def _quarterly_chunks(start_str: str, end_str: str) -> list[str]:
    months = _month_list(start_str, end_str)
    chunks = []
    for i in range(0, len(months), 3):
        g = months[i:i + 3]
        chunks.append(f"{g[0]}_{g[-1]}")
    return chunks


def _seasonal_chunks(start_str: str, end_str: str) -> list[str]:
    """Calendar-quarter-aligned chunks: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end   = datetime.strptime(end_str,   "%Y-%m-%d")
    q_start_month = ((start.month - 1) // 3) * 3 + 1
    curr = start.replace(month=q_start_month, day=1)
    chunks = []
    while curr <= end:
        q_end_month = curr.month + 2
        q_end = curr.replace(month=q_end_month)
        chunks.append(f"{curr.strftime('%Y-%m')}_{q_end.strftime('%Y-%m')}")
        if q_end_month == 12:
            curr = curr.replace(year=curr.year + 1, month=1)
        else:
            curr = curr.replace(month=q_end_month + 1)
    return chunks


def _year_list(start_str: str, end_str: str) -> list[str]:
    s = datetime.strptime(start_str, "%Y-%m-%d")
    e = datetime.strptime(end_str,   "%Y-%m-%d")
    return [str(y) for y in range(s.year, e.year + 1)]