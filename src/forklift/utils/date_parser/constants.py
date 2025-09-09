"""Date parsing constants and format definitions."""

# Common date formats for fallback parsing
COMMON_DATE_FORMATS = [
    "%Y-%m-%d",        # ISO format
    "%d/%m/%Y",        # European format
    "%m/%d/%Y",        # US format
    "%Y/%m/%d",        # Alternative ISO
    "%d-%m-%Y",        # European with dashes
    "%m-%d-%Y",        # US with dashes
    "%Y.%m.%d",        # Dotted format
    "%d.%m.%Y",        # European dotted
    "%Y%m%d",          # Compact format
    "%d-%b-%Y",        # Day-Month-Year with abbreviated month
    "%b %d, %Y",       # Month Day, Year
    "%d %b %Y",        # Day Month Year
]

# Common datetime formats for fallback parsing
COMMON_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",           # ISO datetime
    "%Y-%m-%dT%H:%M:%S",           # ISO with T separator
    "%Y-%m-%d %H:%M:%S.%f",        # ISO with microseconds
    "%Y-%m-%dT%H:%M:%S.%f",        # ISO T with microseconds
    "%Y-%m-%dT%H:%M:%SZ",          # ISO with Z suffix
    "%Y-%m-%dT%H:%M:%S.%fZ",       # ISO with microseconds and Z
    "%Y-%m-%dT%H:%M:%S%z",         # ISO with timezone offset
    "%Y-%m-%dT%H:%M:%S.%f%z",      # ISO with microseconds and timezone
    "%d/%m/%Y %H:%M:%S",           # European datetime
    "%m/%d/%Y %H:%M:%S",           # US datetime
    "%Y/%m/%d %H:%M:%S",           # Alternative ISO datetime
]

# Schema token to strptime format mapping
SCHEMA_TOKEN_MAP = {
    'YYYY': '%Y', 'yyyy': '%Y', 'Yyyy': '%Y',
    'YY': '%y', 'yy': '%y', 'Yy': '%y',
    'MM': '%m', 'mm': '%m', 'Mm': '%m',  # Months (default)
    'M': '%m', 'm': '%m',
    'DD': '%d', 'dd': '%d', 'Dd': '%d',
    'D': '%d', 'd': '%d',
    'HH': '%H', 'hh': '%H', 'Hh': '%H',  # Hours (24-hour)
    'H': '%H', 'h': '%H',
    'SS': '%S', 'ss': '%S', 'Ss': '%S',  # Seconds
    'S': '%S', 's': '%S',
    'MMM': '%b', 'mmm': '%b', 'Mmm': '%b',  # Month abbreviations
    'MMMM': '%B', 'mmmm': '%B', 'Mmmm': '%B',  # Full month names
    'fff': '%f', 'ffffff': '%f',  # Microseconds
}
