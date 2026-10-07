"""The five operator-selected, independent $1 opposite-sticky strategies."""
from dataclasses import asdict, dataclass
import hashlib
import json

STRATEGIES = {"KXSILVER15M":8, "KXSOL15M":7, "KXWTI15M":7,
              "KXDOGE15M":3, "KXHYPE15M":9}
VERSION = "five-opposite-sticky-dollar-v1"

@dataclass(frozen=True)
class DollarConfig:
    series_ticker: str
    subaccount: int = 0
    max_ask: str = "0.9999"
    entry_budget: str = "1.00"
    direction_policy: str = "confirmed"
    version: str = VERSION

    def __post_init__(self):
        if (self.series_ticker not in STRATEGIES or type(self.subaccount) is not int
                or not 0 <= self.subaccount <= 63 or self.entry_budget != "1.00"
                or self.direction_policy != "confirmed" or self.version != VERSION
                or self.max_ask != "0.9999"):
            raise ValueError("INVALID_OPPOSITE_DOLLAR_CONFIGURATION")

    @property
    def history_minutes(self):
        return STRATEGIES[self.series_ticker]

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self),sort_keys=True).encode()).hexdigest()

def portfolio_fingerprint(subaccount=0):
    rows = [asdict(DollarConfig(s,subaccount)) for s in sorted(STRATEGIES)]
    return hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()
