#!/usr/bin/env python3
"""Zero-Lag Event-Driven HTTP & SSE Web Server for ALL Trade UI.

Decouples market tick processing from HTTP connections:
- One dedicated StateBroadcaster evaluates bots whenever WebSocket V3 pushes a tick.
- HTTP GET /api/state serves pre-serialized bytes in <1ms without contention.
- HTTP GET /api/stream wakes immediately on every tick via Condition broadcast.
"""
from __future__ import annotations

from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import logging
from pathlib import Path
import socket
from socketserver import ThreadingMixIn
import sys
import threading
import time
from urllib.parse import urlparse

from .config import BASE_DIR, DEFAULT_PORT, DEFAULT_INITIAL_WALLET, IST
from .market_feed import MarketFeed
from .bot_engine import BotEngineHub

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("server")

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"


class StateBroadcaster:
    """Single-thread engine evaluator & zero-copy JSON publisher for all clients."""

    def __init__(self, feed: MarketFeed, hub: BotEngineHub):
        self.feed = feed
        self.hub = hub
        self._wake = threading.Event()
        self._cond = threading.Condition()
        self._version: int = 0
        self._latest_bytes: bytes = b"{}"
        self._latest_sse_bytes: bytes = b"data: {}\n\n"
        self._running: bool = False
        self._thread: threading.Thread | None = None
        self.publish_now()

    def trigger(self):
        """Wake engine publisher immediately when a new WebSocket tick arrives."""
        self._wake.set()

    def publish_now(self):
        """Synchronously evaluate bot state and publish fresh JSON bytes."""
        snap = self.feed.get_snapshot()
        # During active time-lapse replay loop, MarketFeed evaluates each 1m candle batch via on_replay_candle
        in_live_timelapse = bool(
            snap.get("simulation")
            and self.feed.running
            and self.feed.on_replay_candle is not None
            and self.feed.sim_candles
        )
        if not in_live_timelapse:
            self.hub.update_market_cycle(snap)
        state = self.hub.get_state(snap)
        raw = json.dumps(state).encode("utf-8")
        sse_raw = b"data: " + raw + b"\n\n"
        with self._cond:
            self._latest_bytes = raw
            self._latest_sse_bytes = sse_raw
            self._version += 1
            self._cond.notify_all()

    def start(self):
        if self._running:
            return
        self._running = True
        self.feed.on_update = self.trigger
        self.feed.on_replay_candle = self.hub.update_market_cycle
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._wake.set()
        with self._cond:
            self._cond.notify_all()
        if self._thread:
            self._thread.join(timeout=1.0)

    def _loop(self):
        while self._running:
            # Wait for tick wakeup OR 500ms heartbeat
            self._wake.wait(timeout=0.5)
            self._wake.clear()
            if not self._running:
                break
            try:
                self.publish_now()
            except Exception as e:
                logger.error("StateBroadcaster loop error: %s", e)

    def get_latest_bytes(self) -> bytes:
        with self._cond:
            return self._latest_bytes

    def wait_next_sse(self, last_version: int, timeout: float = 0.5) -> tuple[int, bytes | None]:
        with self._cond:
            if self._version == last_version:
                self._cond.wait(timeout=timeout)
            if self._version == last_version:
                return self._version, None
            return self._version, self._latest_sse_bytes


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Multi-threaded HTTP server handling concurrent SSE and API calls cleanly."""
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        _, exc, _ = sys.exc_info()
        if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


class AllTradeHandler(BaseHTTPRequestHandler):
    """Request handler supporting fast REST endpoints and instant SSE streaming."""

    feed: MarketFeed = None
    hub: BotEngineHub = None
    broadcaster: StateBroadcaster = None

    def log_message(self, format, *args):
        if args and isinstance(args[0], str) and ("/api/stream" in args[0] or "/api/state" in args[0]):
            return
        super().log_message(format, *args)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html"):
            self.handle_index()
        elif path == "/api/state":
            self.handle_api_state()
        elif path == "/api/stream":
            self.handle_api_stream()
        elif path.startswith("/static/"):
            self.handle_static(path)
        else:
            self.send_error(404, "Not Found")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            data = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            data = {}

        if path == "/api/set_wallet":
            self.handle_set_wallet(data)
        elif path == "/api/toggle_bot":
            self.handle_toggle_bot(data)
        elif path == "/api/toggle_all_bots":
            self.handle_toggle_all_bots(data)
        elif path == "/api/reset_bot":
            self.handle_reset_bot(data)
        elif path == "/api/reset_bots":
            self.handle_reset_bots(data)
        elif path == "/api/start_replay":
            self.handle_start_replay(data)
        elif path == "/api/stop_replay":
            self.handle_stop_replay(data)
        elif path == "/api/kill_bot":
            self.handle_kill_bot(data)
        elif path == "/api/kill_all":
            self.handle_kill_all(data)
        elif path == "/api/toggle_simulation":
            self.handle_toggle_simulation(data)
        elif path == "/api/save_arena_archive":
            self.handle_save_arena_archive(data)
        elif path == "/api/add_bot":
            self.handle_add_bot(data)
        else:
            self.send_error(404, "Unknown API endpoint")

    def _sync_publish(self):
        if self.broadcaster is not None:
            self.broadcaster.publish_now()

    def handle_index(self):
        html_file = TEMPLATES_DIR / "index.html"
        if not html_file.exists():
            self.send_error(500, "Dashboard template not found")
            return
        content = html_file.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def handle_static(self, path: str):
        rel_path = path.replace("/static/", "")
        f_path = STATIC_DIR / rel_path
        if not f_path.exists() or not f_path.is_file():
            self.send_error(404, "Static file not found")
            return
        content = f_path.read_bytes()
        mime = "application/javascript" if f_path.suffix == ".js" else "text/css"
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def handle_api_state(self):
        if self.broadcaster is not None:
            payload = self.broadcaster.get_latest_bytes()
        else:
            snap = self.feed.get_snapshot()
            self.hub.update_market_cycle(snap)
            payload = json.dumps(self.hub.get_state(snap)).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def handle_api_stream(self):
        """Instant Server-Sent Events (SSE) push triggered immediately on ticks."""
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        last_ver = -1
        try:
            while True:
                if self.broadcaster is not None:
                    last_ver, sse_msg = self.broadcaster.wait_next_sse(last_ver, timeout=0.5)
                    self.wfile.write(sse_msg if sse_msg is not None else b": keepalive\n\n")
                    self.wfile.flush()
                else:
                    snap = self.feed.get_snapshot()
                    self.hub.update_market_cycle(snap)
                    state = self.hub.get_state(snap)
                    self.wfile.write(f"data: {json.dumps(state)}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(0.1)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def handle_set_wallet(self, data: dict):
        amount = float(data.get("amount", DEFAULT_INITIAL_WALLET))
        self.hub.set_starting_wallet(amount)
        self._sync_publish()
        self._send_json({"status": "ok", "new_wallet": amount})

    def handle_toggle_bot(self, data: dict):
        bot_id = data.get("bot_id", "")
        enabled = data.get("enabled", None)
        res = self.hub.toggle_bot(bot_id, enabled)
        self._sync_publish()
        self._send_json({"status": "ok", "bot_id": bot_id, "enabled": res})

    def handle_toggle_all_bots(self, data: dict):
        enabled = data.get("enabled", None)
        target = self.hub.toggle_all_bots(enabled)
        self._sync_publish()
        self._send_json({"status": "ok", "all_enabled": target})

    def handle_reset_bot(self, data: dict):
        bot_id = data.get("bot_id", "")
        ok = self.hub.reset_bot(bot_id)
        if ok and self.feed._ws_streamer is None:
            self.feed.poll_market_quotes()
        self._sync_publish()
        self._send_json({"status": "ok" if ok else "error", "bot_id": bot_id, "reset": ok})

    def handle_reset_bots(self, data: dict):
        count = self.hub.reset_bots()
        if self.feed._ws_streamer is None:
            self.feed.poll_market_quotes()
        self._sync_publish()
        self._send_json({"status": "ok", "reset_count": count})

    def handle_start_replay(self, data: dict):
        from_date = str(data.get("from_date", "")).strip()
        to_date = str(data.get("to_date", "")).strip()
        if not from_date or not to_date:
            self._send_json({"status": "error", "message": "Both from_date and to_date (YYYY-MM-DD) are required."})
            return
        res = self.feed.start_replay(from_date, to_date)
        if res.get("status") == "ok":
            self.hub.enter_replay_mode()
        self._sync_publish()
        self._send_json(res)

    def handle_stop_replay(self, data: dict):
        res = self.feed.stop_replay()
        self.hub.exit_replay_mode()
        self._sync_publish()
        self._send_json(res)

    def handle_kill_bot(self, data: dict):
        bot_id = data.get("bot_id", "")
        res = self.hub.kill_bot(bot_id)
        self._sync_publish()
        self._send_json({"status": "ok", "bot_id": bot_id, "killed": res})

    def handle_kill_all(self, data: dict):
        count = self.hub.kill_all_bots()
        self._sync_publish()
        self._send_json({"status": "ok", "killed_count": count})

    def handle_toggle_simulation(self, data: dict):
        mode = self.feed.toggle_simulation()
        if mode:
            self.hub.enter_replay_mode()
        else:
            self.hub.exit_replay_mode()
        self._sync_publish()
        self._send_json({"status": "ok", "simulation_mode": mode})

    def handle_save_arena_archive(self, data: dict):
        archive_date = data.get("date")
        folder = self.hub.save_daily_arena_archive(archive_date=archive_date)
        self._send_json({"status": "ok", "folder": str(folder), "date": archive_date or datetime.now(IST).strftime("%Y-%m-%d")})

    def handle_add_bot(self, data: dict):
        bot_id = str(data.get("bot_id", "")).strip()
        name = str(data.get("name", bot_id)).strip()
        family = str(data.get("family", "Custom Strategy")).strip()
        cfg = data.get("cfg") or {
            "capital_floor": 14000.0,
            "daily_profit_lock": 2500.0,
            "daily_loss_limit": 3500.0,
            "max_trades_day": 4,
            "max_lots": 2,
            "target_delta": 0.60,
            "stop_fraction": 0.10,
            "target_fraction": 0.50,
            "step_trail_levels": [[0.085, 0.038], [0.15, 0.095], [0.23, 0.165]],
        }
        enabled = bool(data.get("enabled", True))
        if not bot_id:
            self._send_json({"status": "error", "message": "bot_id is required"})
            return
        self.hub.add_bot(bot_id, name, family, cfg, enabled=enabled)
        self._sync_publish()
        self._send_json({"status": "ok", "bot_id": bot_id, "total_bots": len(self.hub.bots)})

    def _send_json(self, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_server(port: int = DEFAULT_PORT):
    TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    hub = BotEngineHub(initial_wallet=DEFAULT_INITIAL_WALLET, state_file=BASE_DIR / "session_state.json")
    feed = MarketFeed(enable_ws=True, auto_connect=False)
    hub.set_feed_listener(feed.configure_active_consumers)
    broadcaster = StateBroadcaster(feed, hub)
    broadcaster.start()
    feed.start_polling_loop()

    AllTradeHandler.feed = feed
    AllTradeHandler.hub = hub
    AllTradeHandler.broadcaster = broadcaster

    server = ThreadedHTTPServer(("0.0.0.0", port), AllTradeHandler)
    logger.info("ALL Trade UI Server active at http://localhost:%d (WS V3 + Instant SSE Push)", port)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down ALL Trade UI Server...")
    finally:
        broadcaster.stop()
        feed.stop_polling_loop()
        hub.shutdown()
        server.server_close()


if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    run_server(p)
