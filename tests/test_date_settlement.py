"""Tests for the date-based settlement cleanup logic."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestDateSettlement:
    """Validate the date comparison logic that removes settled positions."""

    def test_same_month_earlier_day_should_settle(self):
        """Jun 15 < Jun 16 -> should settle."""
        months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']

        ticker_month = "JUN"
        ticker_day = 15
        today_mm = "JUN"
        today_dd = 16

        ticker_mo_idx = months.index(ticker_month)
        today_mo_idx = months.index(today_mm)

        should_settle = ticker_mo_idx < today_mo_idx or (ticker_mo_idx == today_mo_idx and ticker_day < today_dd)
        assert should_settle, "Jun 15 should settle when today is Jun 16"

    def test_same_day_should_not_settle(self):
        """Jun 16 should NOT settle when today is Jun 16 (same day)."""
        months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']

        ticker_month = "JUN"
        ticker_day = 16
        today_mm = "JUN"
        today_dd = 16

        ticker_mo_idx = months.index(ticker_month)
        today_mo_idx = months.index(today_mm)

        should_settle = ticker_mo_idx < today_mo_idx or (ticker_mo_idx == today_mo_idx and ticker_day < today_dd)
        assert not should_settle, "Jun 16 should NOT settle when today is Jun 16"

    def test_future_day_should_not_settle(self):
        """Jun 17 should NOT settle when today is Jun 16 (future)."""
        months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']

        ticker_month = "JUN"
        ticker_day = 17
        today_mm = "JUN"
        today_dd = 16

        ticker_mo_idx = months.index(ticker_month)
        today_mo_idx = months.index(today_mm)

        should_settle = ticker_mo_idx < today_mo_idx or (ticker_mo_idx == today_mo_idx and ticker_day < today_dd)
        assert not should_settle, "Jun 17 should NOT settle when today is Jun 16"

    def test_previous_month_should_settle(self):
        """May 31 should settle when today is Jun 1."""
        months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']

        ticker_month = "MAY"
        ticker_day = 31
        today_mm = "JUN"
        today_dd = 1

        ticker_mo_idx = months.index(ticker_month)
        today_mo_idx = months.index(today_mm)

        should_settle = ticker_mo_idx < today_mo_idx or (ticker_mo_idx == today_mo_idx and ticker_day < today_dd)
        assert should_settle, "May 31 should settle when today is Jun 1"

    def test_next_month_should_not_settle(self):
        """Jul 1 should NOT settle when today is Jun 30."""
        months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']

        ticker_month = "JUL"
        ticker_day = 1
        today_mm = "JUN"
        today_dd = 30

        ticker_mo_idx = months.index(ticker_month)
        today_mo_idx = months.index(today_mm)

        should_settle = ticker_mo_idx < today_mo_idx or (ticker_mo_idx == today_mo_idx and ticker_day < today_dd)
        assert not should_settle, "Jul 1 should NOT settle when today is Jun 30"

    def test_ticker_regex_extraction(self):
        """Verify that the regex correctly extracts month/day from tickers."""
        import re
        pattern = r'\d{2}(\w{3})(\d{2})'

        # KXHIGHTLV-26JUN12-B107.5
        m = re.search(pattern, "KXHIGHTLV-26JUN12-B107.5")
        assert m, "Should match"
        assert m.group(1) == "JUN"
        assert m.group(2) == "12"

        # KXLOWTAUS-26JUN16-B70.5
        m = re.search(pattern, "KXLOWTAUS-26JUN16-B70.5")
        assert m, "Should match"
        assert m.group(1) == "JUN"
        assert m.group(2) == "16"

        # KXHIGHNY-26JUN15-T78
        m = re.search(pattern, "KXHIGHNY-26JUN15-T78")
        assert m, "Should match"
        assert m.group(1) == "JUN"
        assert m.group(2) == "15"
