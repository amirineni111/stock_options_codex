REFRESH_UNIT_SECONDS = {
    "seconds": 1,
    "minutes": 60,
}


def refresh_interval_to_ms(value: float, unit: str) -> int:
    if unit not in REFRESH_UNIT_SECONDS:
        raise ValueError(f"Unsupported refresh unit: {unit}")
    if value <= 0:
        raise ValueError("Refresh interval must be positive")
    return int(value * REFRESH_UNIT_SECONDS[unit] * 1000)


def format_refresh_interval(value: float, unit: str) -> str:
    label = unit[:-1] if value == 1 and unit.endswith("s") else unit
    return f"{value:g} {label}"
