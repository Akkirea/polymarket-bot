"""
SIGNAL/ZERO Phase 1 — Polymarket API Client

Discovers active 5-minute BTC Up/Down markets via the Gamma API
and tracks their odds in real-time. No API key needed for read-only.

Polymarket 5-min markets:
- New market opens every 5 minutes
- Binary outcome: Up or Down
- Resolved via Chainlink BTC/USD oracle
- "Up" wins if close price >= open price
"""

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from . import config


class PolymarketClient:
    """Read-only client for Polymarket market discovery and odds."""

    def __init__(self):
        self.current_market: Optional[dict] = None
        self.last_poll: Optional[float] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def find_active_5min_market(self) -> Optional[dict]:
        """
        Find the currently active 5-minute BTC Up/Down market.

        Searches the Gamma API for markets matching "Bitcoin Up or Down"
        with 5-minute timeframes, then picks the one whose window
        is currently live.
        """
        session = await self._get_session()

        try:
            # Search for active BTC 5-min markets
            params = {
                "active": "true",
                "closed": "false",
                "limit": 20,
                "order": "startDate",
                "ascending": "false",
            }

            async with session.get(
                f"{config.GAMMA_API_BASE}/markets",
                params=params,
            ) as resp:
                if resp.status != 200:
                    return None
                markets = await resp.json()

            # Filter for 5-minute BTC up/down markets
            btc_5min = []
            for m in markets:
                question = (m.get("question") or "").lower()
                desc = (m.get("description") or "").lower()

                is_btc = "bitcoin" in question or "btc" in question
                is_updown = "up or down" in question or "up/down" in question
                is_5min = (
                    "5:0" in question  # time pattern like "5:00AM-5:05AM"
                    or "5 min" in desc
                    or self._is_5min_window(question)
                )

                if is_btc and is_updown:
                    btc_5min.append(m)

            if not btc_5min:
                # Broader fallback: try event-based search
                return await self._search_by_event()

            # Parse and find the currently active window
            now = datetime.now(timezone.utc)
            for m in btc_5min:
                market = self._parse_market(m)
                if market:
                    return market

            # Return most recent if can't parse windows
            if btc_5min:
                return self._parse_market(btc_5min[0])

            return None

        except Exception as e:
            return None

    async def _search_by_event(self) -> Optional[dict]:
        """Fallback: search events for 5-min crypto markets."""
        session = await self._get_session()
        try:
            params = {
                "active": "true",
                "closed": "false",
                "limit": 50,
                "tag": "crypto",
            }
            async with session.get(
                f"{config.GAMMA_API_BASE}/events",
                params=params,
            ) as resp:
                if resp.status != 200:
                    return None
                events = await resp.json()

            for event in events:
                title = (event.get("title") or "").lower()
                if "bitcoin" in title and ("5" in title or "up" in title):
                    markets = event.get("markets", [])
                    for m in markets:
                        q = (m.get("question") or "").lower()
                        if "up" in q or "down" in q:
                            return self._parse_market(m)
            return None
        except Exception:
            return None

    async def get_orderbook(self, token_id: str) -> Optional[dict]:
        """Fetch order book from CLOB API for a specific token."""
        session = await self._get_session()
        try:
            async with session.get(
                f"{config.CLOB_API_BASE}/book",
                params={"token_id": token_id},
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception:
            return None

    async def poll_market(self) -> Optional[dict]:
        """
        Main polling method. Finds the active market and returns
        current odds + metadata.
        """
        market = await self.find_active_5min_market()
        if market:
            self.current_market = market
            self.last_poll = time.time()
        return market

    def _parse_market(self, raw: dict) -> Optional[dict]:
        """Extract the fields we care about from a raw Gamma API market."""
        try:
            question = raw.get("question", "")
            outcomes = raw.get("outcomes", "")
            outcome_prices = raw.get("outcomePrices", "")

            # Parse outcomes — can be JSON string or list
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    outcomes = ["Up", "Down"]

            if isinstance(outcome_prices, str):
                try:
                    outcome_prices = json.loads(outcome_prices)
                except Exception:
                    outcome_prices = [0.5, 0.5]

            # Map outcomes to prices
            up_price = 0.5
            down_price = 0.5
            for i, outcome in enumerate(outcomes):
                price = float(outcome_prices[i]) if i < len(outcome_prices) else 0.5
                if str(outcome).lower() == "up":
                    up_price = price
                elif str(outcome).lower() == "down":
                    down_price = price

            # Parse time window from question
            window_start, window_end = self._parse_time_window(question)

            return {
                "id": raw.get("id", ""),
                "slug": raw.get("slug", ""),
                "condition_id": raw.get("conditionId", ""),
                "question": question,
                "up_price": up_price,
                "down_price": down_price,
                "volume": float(raw.get("volume", 0) or 0),
                "liquidity": float(raw.get("liquidityNum", 0) or 0),
                "window_start": window_start,
                "window_end": window_end,
                "end_date": raw.get("endDate", ""),
                "tokens": raw.get("clobTokenIds", ""),
            }

        except Exception:
            return None

    async def get_resolution(
        self, market_id: str, slug: str = ""
    ) -> tuple:
        """
        Fetch authoritative settlement data for a closed market.

        Returns (winner, final_price, price_to_beat) where winner is "up" or
        "down" (lowercase). Returns (None, None, None) if not yet resolved.

        Tries eventMetadata.finalPrice vs priceToBeat first (authoritative),
        then falls back to outcomePrices >= 0.99 for markets that resolve
        without publishing finalPrice.
        """
        session = await self._get_session()
        params = {"slug": slug} if slug else {"id": market_id}
        try:
            async with session.get(
                f"{config.GAMMA_API_BASE}/markets", params=params
            ) as resp:
                if resp.status != 200:
                    return None, None, None
                markets = await resp.json()
        except Exception:
            return None, None, None

        if not markets:
            return None, None, None

        m = markets[0]
        if not m.get("closed"):
            return None, None, None

        # ── Primary: eventMetadata.finalPrice vs priceToBeat ──────────────
        events = m.get("events", [])
        meta = (events[0].get("eventMetadata") or {}) if events else {}

        final_price: Optional[float] = None
        price_to_beat: Optional[float] = None

        try:
            if meta.get("finalPrice") is not None:
                final_price = float(meta["finalPrice"])
            if meta.get("priceToBeat") is not None:
                price_to_beat = float(meta["priceToBeat"])
        except Exception:
            pass

        if final_price is not None and price_to_beat is not None:
            winner = "up" if final_price >= price_to_beat else "down"
            return winner, final_price, price_to_beat

        # ── Fallback: outcomePrices settled to 0/1 ─────────────────────────
        try:
            outcomes = json.loads(m.get("outcomes", "[]"))
            outcome_prices = json.loads(m.get("outcomePrices", "[]"))
            for i, price_str in enumerate(outcome_prices):
                if float(price_str) >= 0.99 and i < len(outcomes):
                    winner = str(outcomes[i]).lower()
                    return winner, None, None
        except Exception:
            pass

        return None, None, None

    def _parse_time_window(self, question: str) -> tuple:
        """
        Extract start/end times from question like:
        "Bitcoin Up or Down - March 23, 5:30AM-5:35AM ET"
        """
        # Look for time range pattern
        pattern = r"(\d{1,2}:\d{2}[AP]M)\s*[-–]\s*(\d{1,2}:\d{2}[AP]M)"
        match = re.search(pattern, question, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2)
        return "", ""

    def _is_5min_window(self, question: str) -> bool:
        """Check if the time window in the question spans ~5 minutes."""
        start, end = self._parse_time_window(question)
        if not start or not end:
            return False
        # Simple heuristic: parse minutes and check difference
        try:
            s_min = int(start.split(":")[1][:2])
            e_min = int(end.split(":")[1][:2])
            diff = (e_min - s_min) % 60
            return diff == 5
        except Exception:
            return False
