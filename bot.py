import asyncio
import html
import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
import time

import aiohttp
from dotenv import load_dotenv
from aiogram.exceptions import TelegramForbiddenError

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    LabeledPrice,
    MessageEntity,
    ReplyKeyboardMarkup,
    User,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set BOT_TOKEN in .env or environment variables.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MEDIA_DIR = Path("media")
MEDIA_DIR.mkdir(exist_ok=True)

MAX_MESSAGES_PER_CHAT = 500
MAX_MEDIA_FILES = 300

DB_PATH = Path(os.getenv("DB_PATH", "bot.db"))

OWNER_ID: Optional[int] = None

ADMIN_ID = 6201234513

PREMIUM_CUSTOM_EMOJI_ID = "5299021192563268524"

STARS_CURRENCY = "XTR"
STARS_DURATIONS: dict[str, int] = {
    "7d": 7 * 24 * 60 * 60,
    "14d": 14 * 24 * 60 * 60,
    "30d": 30 * 24 * 60 * 60,
}
DEFAULT_STARS_PRICES: dict[str, int] = {"7d": 15, "14d": 25, "30d": 45}

# Dedup notifications (prevents double sends when Telegram delivers the same business event twice)
_RECENT_NOTIFY: dict[tuple[Any, ...], float] = {}
_RECENT_NOTIFY_TTL_SEC = 120.0


def _conv_pair(a: Optional[int], b: Optional[int]) -> Optional[tuple[int, int]]:
    if a is None or b is None:
        return None
    try:
        a_i = int(a)
        b_i = int(b)
    except Exception:
        return None
    return (a_i, b_i) if a_i <= b_i else (b_i, a_i)

SUPPORT_CHAT_ID = -5226762204

connected_chats: set[int] = set()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

_BOT_USERNAME_CACHE: Optional[str] = None
_ACCESS_CODE_CACHE: tuple[Optional[str], float] = (None, 0.0)

_DEBUG_LAST_BUSINESS_UPDATE_ID: Optional[int] = None


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
    except Exception:
        # Best-effort pragmas
        pass
    return conn


async def _get_bot_username() -> str:
    global _BOT_USERNAME_CACHE
    if _BOT_USERNAME_CACHE:
        return _BOT_USERNAME_CACHE
    me = await bot.get_me()
    _BOT_USERNAME_CACHE = me.username or "your_bot"
    return _BOT_USERNAME_CACHE


def _db_init() -> None:
    with _db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY,
                paid_until INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS free_users (
                user_id INTEGER PRIMARY KEY,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                path TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS forwarded (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY(chat_id, message_id, tag)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                name TEXT,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                blocked INTEGER NOT NULL DEFAULT 0,
                bot_user INTEGER NOT NULL DEFAULT 0,
                whitelisted INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                chat_id INTEGER,
                message_id INTEGER,
                created_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS support_state (
                user_id INTEGER PRIMARY KEY,
                pending INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS broadcast_state (
                user_id INTEGER PRIMARY KEY,
                pending INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS access_state (
                user_id INTEGER PRIMARY KEY,
                pending INTEGER NOT NULL DEFAULT 0,
                kind TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notify_recipients (
                user_id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                owner_user_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                notified INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS business_bind_state (
                user_id INTEGER PRIMARY KEY,
                pending INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS owner_chats (
                owner_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                PRIMARY KEY(owner_user_id, chat_id)
            )
            """
        )

        # Ensure admin receives notifications by default
        try:
            conn.execute(
                "INSERT INTO notify_recipients(user_id, enabled, created_at) VALUES(?, 1, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET enabled=1",
                (int(ADMIN_ID), int(datetime.utcnow().timestamp())),
            )
        except Exception:
            logger.exception("Failed to ensure admin notify recipient")

        # Lightweight migration for existing DBs
        try:
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
            if "blocked" not in cols:
                conn.execute("ALTER TABLE users ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0")
            if "bot_user" not in cols:
                conn.execute("ALTER TABLE users ADD COLUMN bot_user INTEGER NOT NULL DEFAULT 0")
            if "whitelisted" not in cols:
                conn.execute("ALTER TABLE users ADD COLUMN whitelisted INTEGER NOT NULL DEFAULT 0")
        except Exception:
            logger.exception("Failed to migrate users table")

        try:
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(business_connections)").fetchall()]
            if "notified" not in cols:
                conn.execute("ALTER TABLE business_connections ADD COLUMN notified INTEGER NOT NULL DEFAULT 0")
        except Exception:
            logger.exception("Failed to migrate business_connections table")

        try:
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(subscriptions)").fetchall()]
            if "paid_until" not in cols:
                # Should not happen, but keep migration shape consistent.
                conn.execute("ALTER TABLE subscriptions ADD COLUMN paid_until INTEGER NOT NULL DEFAULT 0")
        except Exception:
            # Table may not exist in older DBs; create above covers it.
            pass


def _db_get_paid_mode() -> bool:
    try:
        with _db() as conn:
            row = conn.execute("SELECT value FROM kv WHERE key='paid_mode'").fetchone()
            if not row:
                return False
            return str(row["value"]).strip() in {"1", "true", "yes"}
    except Exception:
        logger.exception("Failed to get paid_mode")
        return False


def _db_set_paid_mode(enabled: bool) -> None:
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES('paid_mode', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("1" if enabled else "0",),
            )
    except Exception:
        logger.exception("Failed to set paid_mode")


def _db_is_free_user(user_id: int) -> bool:
    try:
        with _db() as conn:
            row = conn.execute("SELECT 1 FROM free_users WHERE user_id=?", (int(user_id),)).fetchone()
            return row is not None
    except Exception:
        logger.exception("Failed to check free user")
        return False


def _db_set_free_user(user_id: int, enabled: bool) -> None:
    try:
        with _db() as conn:
            if enabled:
                conn.execute(
                    "INSERT OR IGNORE INTO free_users(user_id, created_at) VALUES(?, ?)",
                    (int(user_id), int(datetime.utcnow().timestamp())),
                )
            else:
                conn.execute("DELETE FROM free_users WHERE user_id=?", (int(user_id),))
    except Exception:
        logger.exception("Failed to set free user")


def _db_list_free_users(limit: int = 50) -> list[int]:
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT user_id FROM free_users ORDER BY created_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [int(r["user_id"]) for r in rows]
    except Exception:
        logger.exception("Failed to list free users")
        return []


def _db_sub_get_paid_until(user_id: int) -> int:
    try:
        with _db() as conn:
            row = conn.execute("SELECT paid_until FROM subscriptions WHERE user_id=?", (int(user_id),)).fetchone()
            return int(row["paid_until"]) if row else 0
    except Exception:
        logger.exception("Failed to get subscription")
        return 0


def _db_sub_extend(user_id: int, seconds: int) -> int:
    now = int(datetime.utcnow().timestamp())
    cur = _db_sub_get_paid_until(int(user_id))
    base = cur if cur > now else now
    new_until = int(base + int(seconds))
    try:
        with _db() as conn:
            conn.execute(
                """
                INSERT INTO subscriptions(user_id, paid_until, created_at, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    paid_until=excluded.paid_until,
                    updated_at=excluded.updated_at
                """,
                (int(user_id), int(new_until), now, now),
            )
    except Exception:
        logger.exception("Failed to extend subscription")
    return new_until


def _has_active_subscription(user_id: int) -> bool:
    if int(user_id) == int(ADMIN_ID):
        return True
    if not _db_get_paid_mode():
        return True
    if _db_is_free_user(int(user_id)):
        return True
    until = _db_sub_get_paid_until(int(user_id))
    now = int(datetime.utcnow().timestamp())
    return int(until) > now


def _fmt_dt(ts: int) -> str:
    try:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)


def _user_display(u: dict) -> str:
    username = u.get("username")
    name = " ".join([p for p in [u.get("first_name"), u.get("last_name")] if p]).strip()
    if username:
        return f"@{username}"
    return name or str(u.get("id") or "")


def _db_touch_user(u: dict) -> None:
    user_id = u.get("id")
    if not user_id:
        return
    now = int(datetime.utcnow().timestamp())
    username = u.get("username")
    name = " ".join([p for p in [u.get("first_name"), u.get("last_name")] if p]).strip() or None
    try:
        with _db() as conn:
            conn.execute(
                """
                INSERT INTO users(user_id, username, name, first_seen, last_seen)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    name=excluded.name,
                    last_seen=excluded.last_seen
                """,
                (int(user_id), username, name, now, now),
            )
    except Exception:
        logger.exception("Failed to touch user")


def _db_set_business_connection(connection_id: str, owner_user_id: int) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                """
                INSERT INTO business_connections(connection_id, owner_user_id, created_at, updated_at, notified)
                VALUES(?, ?, ?, ?, 0)
                ON CONFLICT(connection_id) DO UPDATE SET
                    owner_user_id=excluded.owner_user_id,
                    updated_at=excluded.updated_at
                """,
                (str(connection_id), int(owner_user_id), now, now),
            )
    except Exception:
        logger.exception("Failed to set business connection mapping")


def _db_upsert_business_connection_safe(connection_id: str, owner_user_id: int) -> bool:
    """Insert mapping if missing. If present, overwrite only when previous owner is not whitelisted.

    Returns True if mapping was inserted/changed.
    """
    cid = str(connection_id)
    new_owner = int(owner_user_id)
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT owner_user_id, notified FROM business_connections WHERE connection_id=?",
                (cid,),
            ).fetchone()
            if not row:
                conn.execute(
                    "INSERT INTO business_connections(connection_id, owner_user_id, created_at, updated_at, notified) VALUES(?, ?, ?, ?, 0)",
                    (cid, new_owner, now, now),
                )
                return True

            old_owner = int(row["owner_user_id"])
            if old_owner == new_owner:
                return False

            # Allow remap only if the old owner never started the bot (stale/garbage mapping)
            # Special case: allow remap away from the main admin if it was previously force-bound.
            if old_owner != int(ADMIN_ID) and _db_is_bot_user(old_owner):
                return False

            # Only allow remap TO a real bot user with access (prevents hijacking)
            if not _db_is_bot_user(new_owner):
                return False
            if not _db_is_whitelisted(new_owner):
                return False

            conn.execute(
                "UPDATE business_connections SET owner_user_id=?, updated_at=?, notified=0 WHERE connection_id=?",
                (new_owner, now, cid),
            )
            return True
    except Exception:
        logger.exception("Failed to upsert business connection safely")
        return False


def _db_is_bot_user(user_id: int) -> bool:
    """True if user has interacted with bot in private chat (so bot can PM them)."""
    try:
        with _db() as conn:
            row = conn.execute("SELECT bot_user FROM users WHERE user_id=?", (int(user_id),)).fetchone()
            return bool(row["bot_user"]) if row else False
    except Exception:
        logger.exception("Failed to check bot_user")
        return False


def _db_mark_connection_notified(connection_id: str) -> bool:
    """Returns True if this is the first time we mark this connection as notified."""
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT notified FROM business_connections WHERE connection_id=?",
                (str(connection_id),),
            ).fetchone()
            if not row:
                return False
            if int(row["notified"]) == 1:
                return False
            conn.execute(
                "UPDATE business_connections SET notified=1 WHERE connection_id=?",
                (str(connection_id),),
            )
            return True
    except Exception:
        logger.exception("Failed to mark business connection notified")
        return False


def _db_get_owner_by_connection(connection_id: str) -> Optional[int]:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT owner_user_id FROM business_connections WHERE connection_id=?",
                (str(connection_id),),
            ).fetchone()
            return int(row["owner_user_id"]) if row else None
    except Exception:
        logger.exception("Failed to get owner by business connection")
        return None


def _db_business_bind_set_pending(user_id: int, pending: bool) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO business_bind_state(user_id, pending, created_at) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET pending=excluded.pending, created_at=excluded.created_at",
                (int(user_id), 1 if pending else 0, now),
            )
    except Exception:
        logger.exception("Failed to set business bind pending")


def _db_business_bind_is_pending(user_id: int) -> bool:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT pending FROM business_bind_state WHERE user_id=?",
                (int(user_id),),
            ).fetchone()
            return bool(row["pending"]) if row else False
    except Exception:
        logger.exception("Failed to get business bind pending")
        return False


def _db_business_bind_pick_pending_owner() -> Optional[int]:
    """Pick the most recently requested pending business bind owner.

    This is used when we observe a business_message that contains a business_connection_id,
    because the sender of that message can be the external chat participant (not the owner).
    """
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT user_id FROM business_bind_state WHERE pending=1 ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            return int(row["user_id"])
    except Exception:
        logger.exception("Failed to pick pending business bind owner")
        return None


def _db_business_bind_clear(user_id: int) -> None:
    try:
        with _db() as conn:
            conn.execute("UPDATE business_bind_state SET pending=0 WHERE user_id=?", (int(user_id),))
    except Exception:
        logger.exception("Failed to clear business bind pending")


def _db_count_business_connections_for_owner(owner_user_id: int) -> int:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM business_connections WHERE owner_user_id=?",
                (int(owner_user_id),),
            ).fetchone()
            return int(row["c"]) if row else 0
    except Exception:
        logger.exception("Failed to count business connections for owner")
        return 0


def _db_owner_chat_add(owner_user_id: int, chat_id: int) -> bool:
    """Returns True if chat is new for this owner."""
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            cur = conn.execute(
                "UPDATE owner_chats SET last_seen=? WHERE owner_user_id=? AND chat_id=?",
                (now, int(owner_user_id), int(chat_id)),
            )
            if cur.rowcount and cur.rowcount > 0:
                return False
            conn.execute(
                "INSERT INTO owner_chats(owner_user_id, chat_id, first_seen, last_seen) VALUES(?, ?, ?, ?)",
                (int(owner_user_id), int(chat_id), now, now),
            )
            return True
    except Exception:
        logger.exception("Failed to add owner chat")
        return False


def _db_owner_chat_list_owners(chat_id: int) -> list[int]:
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT owner_user_id FROM owner_chats WHERE chat_id=?",
                (int(chat_id),),
            ).fetchall()
            out: list[int] = []
            for r in rows:
                try:
                    out.append(int(r["owner_user_id"]))
                except Exception:
                    continue
            return out
    except Exception:
        logger.exception("Failed to list owners by chat")
        return []


def _db_owner_chat_has(owner_user_id: int, chat_id: int) -> bool:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT 1 FROM owner_chats WHERE owner_user_id=? AND chat_id=? LIMIT 1",
                (int(owner_user_id), int(chat_id)),
            ).fetchone()
            return row is not None
    except Exception:
        logger.exception("Failed to check owner chat")
        return False


def _notify_recipients_for_chat(*, owner_id: Optional[int], chat_id: int) -> set[int]:
    """Privacy-safe recipients for a business chat.

    - Always notify the owner of the business_connection (their "world").
    - Additionally notify the other participant ONLY if it's a mutual business chat between two bot owners,
      i.e. the other participant is a bot user and has this owner in their owner_chats.
    """
    recipients: set[int] = set()
    if owner_id is not None:
        recipients.add(int(owner_id))

    # Mutual owner-owner chat case
    other = int(chat_id)
    if owner_id is not None and _db_is_bot_user(other) and _db_owner_chat_has(other, int(owner_id)):
        recipients.add(other)
    return recipients


def _db_owner_chat_count(owner_user_id: int) -> int:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM owner_chats WHERE owner_user_id=?",
                (int(owner_user_id),),
            ).fetchone()
            return int(row["c"]) if row else 0
    except Exception:
        logger.exception("Failed to count owner chats")
        return 0


def _extract_business_connection_id(obj: dict) -> Optional[str]:
    """Best-effort extraction of business connection id from various payload shapes."""
    if not isinstance(obj, dict):
        return None
    for k in ("business_connection_id", "business_connectionId", "connection_id", "connectionId"):
        v = obj.get(k)
        if v is not None:
            return str(v)
    bc = obj.get("business_connection")
    if isinstance(bc, dict):
        for k in ("id", "business_connection_id", "connection_id"):
            v = bc.get(k)
            if v is not None:
                return str(v)
    return None


def _target_owner_for_business(obj: dict) -> Optional[int]:
    cid = _extract_business_connection_id(obj)
    if cid:
        owner = _db_get_owner_by_connection(cid)
        if owner:
            return owner
    # No fallback to global OWNER_ID for business events (privacy-safe)
    return None


def _db_mark_bot_user(user_id: int) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO users(user_id, bot_user, first_seen, last_seen) VALUES(?, 1, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET bot_user=1, last_seen=?",
                (int(user_id), now, now, now),
            )
    except Exception:
        logger.exception("Failed to mark bot user")


def _db_is_whitelisted(user_id: int) -> bool:
    if int(user_id) == int(ADMIN_ID):
        return True
    # If no access code is set, run in open multi-user mode.
    # Anyone who started the bot can use it for their own Telegram Business chats.
    try:
        if not _db_get_access_code():
            return True
    except Exception:
        # fall back to DB flag
        pass
    try:
        with _db() as conn:
            row = conn.execute("SELECT whitelisted FROM users WHERE user_id=?", (int(user_id),)).fetchone()
            return bool(row["whitelisted"]) if row else False
    except Exception:
        logger.exception("Failed to check whitelisted")
        return False


def _db_set_whitelisted(user_id: int, whitelisted: bool) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO users(user_id, whitelisted, first_seen, last_seen) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET whitelisted=excluded.whitelisted, last_seen=?",
                (int(user_id), 1 if whitelisted else 0, now, now, now),
            )
    except Exception:
        logger.exception("Failed to set whitelisted")


def _db_notify_set(user_id: int, enabled: bool) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO notify_recipients(user_id, enabled, created_at) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET enabled=excluded.enabled",
                (int(user_id), 1 if enabled else 0, now),
            )
    except Exception:
        logger.exception("Failed to set notify recipient")


def _db_notify_list_recipients() -> list[int]:
    """Recipients who should receive business notifications.

    Only whitelisted + not blocked users are allowed.
    """
    try:
        with _db() as conn:
            rows = conn.execute(
                """
                SELECT nr.user_id
                FROM notify_recipients nr
                JOIN users u ON u.user_id = nr.user_id
                WHERE nr.enabled=1 AND u.whitelisted=1 AND u.blocked=0
                ORDER BY nr.user_id ASC
                """
            ).fetchall()
            ids = [int(r["user_id"]) for r in rows]
            if int(ADMIN_ID) not in ids:
                ids.insert(0, int(ADMIN_ID))
            return ids
    except Exception:
        logger.exception("Failed to list notify recipients")
        return [int(ADMIN_ID)]


def _db_get_access_code() -> Optional[str]:
    global _ACCESS_CODE_CACHE
    cached_value, cached_ts = _ACCESS_CODE_CACHE
    if cached_ts and (time.time() - cached_ts) < 60:
        return cached_value
    try:
        with _db() as conn:
            row = conn.execute("SELECT value FROM kv WHERE key='access_code'").fetchone()
            if not row:
                _ACCESS_CODE_CACHE = (None, time.time())
                return None
            v = str(row["value"]).strip()
            res = v or None
            _ACCESS_CODE_CACHE = (res, time.time())
            return res
    except Exception:
        logger.exception("Failed to get access code")
        return None


def _db_set_access_code(code: str) -> None:
    global _ACCESS_CODE_CACHE
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES('access_code', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(code).strip(),),
            )
        _ACCESS_CODE_CACHE = (str(code).strip(), time.time())
    except Exception:
        logger.exception("Failed to set access code")


def _db_access_set_pending(user_id: int, pending: bool, kind: str) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO access_state(user_id, pending, kind, created_at) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET pending=excluded.pending, kind=excluded.kind, created_at=excluded.created_at",
                (int(user_id), 1 if pending else 0, str(kind), now),
            )
    except Exception:
        logger.exception("Failed to set access pending")


def _db_access_get_pending(user_id: int) -> Optional[str]:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT pending, kind FROM access_state WHERE user_id=?",
                (int(user_id),),
            ).fetchone()
            if not row:
                return None
            if not bool(row["pending"]):
                return None
            return str(row["kind"])
    except Exception:
        logger.exception("Failed to get access pending")
        return None


def _db_support_set_pending(user_id: int, pending: bool) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO support_state(user_id, pending, created_at) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET pending=excluded.pending, created_at=excluded.created_at",
                (int(user_id), 1 if pending else 0, now),
            )
    except Exception:
        logger.exception("Failed to set support pending")


def _db_support_is_pending(user_id: int) -> bool:
    try:
        with _db() as conn:
            row = conn.execute("SELECT pending FROM support_state WHERE user_id=?", (int(user_id),)).fetchone()
            return bool(row["pending"]) if row else False
    except Exception:
        logger.exception("Failed to get support pending")
        return False


def _db_broadcast_set_pending(user_id: int, pending: bool) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO broadcast_state(user_id, pending, created_at) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET pending=excluded.pending, created_at=excluded.created_at",
                (int(user_id), 1 if pending else 0, now),
            )
    except Exception:
        logger.exception("Failed to set broadcast pending")


def _db_broadcast_is_pending(user_id: int) -> bool:
    try:
        with _db() as conn:
            row = conn.execute("SELECT pending FROM broadcast_state WHERE user_id=?", (int(user_id),)).fetchone()
            return bool(row["pending"]) if row else False
    except Exception:
        logger.exception("Failed to get broadcast pending")
        return False


async def _send_support_prompt(user_id: int) -> None:
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="support_cancel")]]
    )
    text = (
        "🆘 Техподдержка\n\n"
        "Опишите проблему одним сообщением и отправьте сюда. "
        "Ваше сообщение будет передано в поддержку."
    )
    await bot.send_message(int(user_id), text, reply_markup=kb)


def _main_menu_kb(*, is_admin: bool) -> ReplyKeyboardMarkup:
    rows: list[list[KeyboardButton]] = [
        [KeyboardButton(text="📌 Инструкция"), KeyboardButton(text="📊 Статус")],
        [KeyboardButton(text="🔒 Конфиденциальность"), KeyboardButton(text="🆘 Техподдержка")],
        [KeyboardButton(text="🔗 Подключить бизнес")],
        [KeyboardButton(text="⭐ Купить доступ")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="👑 Админка"), KeyboardButton(text="🚫 Черный список")])
        rows.append([KeyboardButton(text="📣 Рассылка"), KeyboardButton(text="❌ Отмена")])
        rows.append([KeyboardButton(text="🔑 Код доступа")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)


def _db_list_bot_user_ids() -> list[int]:
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT user_id FROM users WHERE bot_user=1 AND blocked=0 AND whitelisted=1"
            ).fetchall()
            return [int(r["user_id"]) for r in rows]
    except Exception:
        logger.exception("Failed to list bot user ids")
        return []


async def _send_help(chat_id: int) -> None:
    bot_username = await _get_bot_username()
    text = (
        "📌 Инструкция\n\n"
        "Важно: для работы бизнес-чатботов нужен <b>Telegram Premium</b> (функция <b>Telegram для бизнеса</b>).\n\n"
        "1) Откройте Telegram → <b>Telegram для бизнеса</b>\n"
        "2) Перейдите в <b>Чат-боты</b>\n"
        f"3) Добавьте <code>@{bot_username}</code>\n\n"
        "После этого просто начните переписку (вы можете написать кому-то сами или вам могут написать) — бот начнёт получать бизнес-апдейты."
    )
    await bot.send_message(int(chat_id), text)


async def _send_broadcast_prompt(chat_id: int) -> None:
    text = (
        "📣 Рассылка\n\n"
        "Отправь одно сообщение (текст/фото/видео и т.д.). Я разошлю его всем пользователям бота.\n\n"
        "Чтобы отменить — нажми ❌ Отмена."
    )
    await bot.send_message(int(chat_id), text)


async def _send_status(chat_id: int) -> None:
    # Privacy-safe status
    is_admin = int(chat_id) == int(ADMIN_ID)
    connections_count = _db_count_business_connections_for_owner(int(chat_id))
    chats_count = _db_owner_chat_count(int(chat_id))
    wl = _db_is_whitelisted(int(chat_id))

    lines = ["📊 Статус", "", f"• Ваш ID: {int(chat_id)}"]
    lines.append(f"• Доступ: {'✅ открыт' if wl or is_admin else '🔐 закрыт'}")
    if int(chat_id) == int(ADMIN_ID):
        lines.append("• Подписка: 👑 админ (бесплатно)")
    else:
        until = _db_sub_get_paid_until(int(chat_id))
        if until > int(datetime.utcnow().timestamp()):
            lines.append(f"• Подписка: ✅ активна до {_fmt_dt(until)}")
        else:
            lines.append("• Подписка: ❌ нет")
    lines.append(f"• Бизнес-подключений (для вашего аккаунта): {connections_count}")
    lines.append(f"• Подключённые бизнес-чаты (для вашего аккаунта): {chats_count}")

    if is_admin:
        owner = _db_get_owner_id()
        lines.append(f"• OWNER_ID (владелец/админ): {owner if owner else 'не задан'}")

    await bot.send_message(int(chat_id), "\n".join(lines))


async def _send_privacy(chat_id: int) -> None:
    text = (
        "🔒 Конфиденциальность и безопасность\n\n"
        "Этот бот не передаёт содержимое ваших сообщений сторонним сервисам.\n\n"
        "Важно: работа через <b>Telegram для бизнеса</b> доступна только при наличии <b>Telegram Premium</b>.\n\n"
        "Бот взаимодействует только с официальным Telegram Bot API, чтобы получать обновления и отправлять уведомления."
    )
    await bot.send_message(int(chat_id), text)


def _db_log_event(*, user_id: Optional[int], action: str, chat_id: Optional[int] = None, message_id: Optional[int] = None) -> None:
    now = int(datetime.utcnow().timestamp())
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO events(user_id, action, chat_id, message_id, created_at) VALUES(?, ?, ?, ?, ?)",
                (
                    int(user_id) if user_id else None,
                    str(action),
                    int(chat_id) if chat_id is not None else None,
                    int(message_id) if message_id is not None else None,
                    now,
                ),
            )
    except Exception:
        logger.exception("Failed to log event")


def _db_stats_users_count() -> int:
    try:
        with _db() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM users WHERE bot_user=1").fetchone()
            return int(row["c"]) if row else 0
    except Exception:
        logger.exception("Failed to count users")
        return 0


def _db_is_blocked(user_id: int) -> bool:
    try:
        with _db() as conn:
            row = conn.execute("SELECT blocked FROM users WHERE user_id=?", (int(user_id),)).fetchone()
            return bool(row["blocked"]) if row else False
    except Exception:
        logger.exception("Failed to check blocked")
        return False


def _is_blocked_effective(user_id: int) -> bool:
    try:
        if int(user_id) == int(ADMIN_ID):
            return False
        return _db_is_blocked(int(user_id))
    except Exception:
        return False


def _db_set_blocked(user_id: int, blocked: bool) -> None:
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO users(user_id, blocked, first_seen, last_seen) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET blocked=excluded.blocked",
                (int(user_id), 1 if blocked else 0, int(datetime.utcnow().timestamp()), int(datetime.utcnow().timestamp())),
            )
    except Exception:
        logger.exception("Failed to set blocked")


def _db_list_users(limit: int = 10, offset: int = 0) -> list[sqlite3.Row]:
    try:
        with _db() as conn:
            return conn.execute(
                """
                SELECT user_id, username, name, first_seen, last_seen, blocked
                FROM users
                WHERE bot_user=1
                ORDER BY last_seen DESC
                LIMIT ? OFFSET ?
                """,
                (int(limit), int(offset)),
            ).fetchall()
    except Exception:
        logger.exception("Failed to list users")
        return []


def _db_list_blocked_users(limit: int = 50, offset: int = 0) -> list[sqlite3.Row]:
    try:
        with _db() as conn:
            return conn.execute(
                """
                SELECT user_id, username, name, first_seen, last_seen, blocked
                FROM users
                WHERE blocked=1
                ORDER BY last_seen DESC
                LIMIT ? OFFSET ?
                """,
                (int(limit), int(offset)),
            ).fetchall()
    except Exception:
        logger.exception("Failed to list blocked users")
        return []


def _db_stats_actions(limit: int = 8) -> list[tuple[str, int]]:
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT action, COUNT(*) AS c FROM events GROUP BY action ORDER BY c DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [(str(r["action"]), int(r["c"])) for r in rows]
    except Exception:
        logger.exception("Failed to load action stats")
        return []


def _db_recent_events(limit: int = 15) -> list[sqlite3.Row]:
    try:
        with _db() as conn:
            return conn.execute(
                """
                SELECT e.action, e.created_at, e.user_id, u.username, u.name
                FROM events e
                LEFT JOIN users u ON u.user_id = e.user_id
                ORDER BY e.id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    except Exception:
        logger.exception("Failed to load recent events")
        return []


def _db_get_owner_id() -> Optional[int]:
    try:
        with _db() as conn:
            row = conn.execute("SELECT value FROM kv WHERE key='owner_id'").fetchone()
            if not row:
                return None
            return int(row["value"])
    except Exception:
        logger.exception("Failed to get owner id from db")
        return None


def _db_set_owner_id(owner_id: int) -> None:
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES('owner_id', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(int(owner_id)),),
            )
    except Exception:
        logger.exception("Failed to set owner id in db")


def _db_put_message(chat_id: int, message_id: int, payload: dict) -> None:
    try:
        now = int(datetime.utcnow().timestamp())
        with _db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO messages(chat_id, message_id, payload_json, created_at) VALUES(?,?,?,?)",
                (int(chat_id), int(message_id), json.dumps(payload, ensure_ascii=False), now),
            )
    except Exception:
        logger.exception("Failed to put message into db")


def _db_get_message(chat_id: int, message_id: int) -> Optional[dict]:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT payload_json FROM messages WHERE chat_id=? AND message_id=?",
                (int(chat_id), int(message_id)),
            ).fetchone()
            if not row:
                return None
            return json.loads(row["payload_json"])
    except Exception:
        logger.exception("Failed to get message from db")
        return None


def _db_delete_message(chat_id: int, message_id: int) -> None:
    try:
        with _db() as conn:
            conn.execute("DELETE FROM messages WHERE chat_id=? AND message_id=?", (int(chat_id), int(message_id)))
    except Exception:
        logger.exception("Failed to delete message from db")


def _db_set_media(chat_id: int, message_id: int, kind: str, path: str) -> None:
    try:
        now = int(datetime.utcnow().timestamp())
        with _db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO media(chat_id, message_id, kind, path, created_at) VALUES(?,?,?,?,?)",
                (int(chat_id), int(message_id), str(kind), str(path), now),
            )
    except Exception:
        logger.exception("Failed to set media in db")


def _db_get_media(chat_id: int, message_id: int) -> Optional[dict]:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT kind, path FROM media WHERE chat_id=? AND message_id=?",
                (int(chat_id), int(message_id)),
            ).fetchone()
            if not row:
                return None
            return {"kind": row["kind"], "path": row["path"]}
    except Exception:
        logger.exception("Failed to get media from db")
        return None


def _db_delete_media(chat_id: int, message_id: int) -> None:
    try:
        with _db() as conn:
            conn.execute("DELETE FROM media WHERE chat_id=? AND message_id=?", (int(chat_id), int(message_id)))
    except Exception:
        logger.exception("Failed to delete media from db")


def _db_trim_chat(chat_id: int) -> None:
    # Keep only last MAX_MESSAGES_PER_CHAT
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT message_id FROM messages WHERE chat_id=? ORDER BY created_at DESC LIMIT -1 OFFSET ?",
                (int(chat_id), MAX_MESSAGES_PER_CHAT),
            ).fetchall()
            if not rows:
                return

        ids = [int(r["message_id"]) for r in rows]
        for mid in ids:
            mp = _db_get_media(chat_id, mid)
            if mp:
                _unlink_quiet(str(mp["path"]))
                _db_delete_media(chat_id, mid)
            _db_delete_message(chat_id, mid)
    except Exception:
        logger.exception("Failed to trim chat cache")


def _db_trim_media() -> None:
    # Keep only last MAX_MEDIA_FILES media rows
    try:
        with _db() as conn:
            rows = conn.execute(
                """
                SELECT chat_id, message_id, path FROM media ORDER BY created_at DESC LIMIT -1 OFFSET ?
                """,
                (MAX_MEDIA_FILES,),
            ).fetchall()
            for r in rows:
                _unlink_quiet(str(r["path"]))
                conn.execute("DELETE FROM media WHERE chat_id=? AND message_id=?", (int(r["chat_id"]), int(r["message_id"])))
    except Exception:
        logger.exception("Failed to trim media")


def _db_clear_cache() -> tuple[int, int, int]:
    try:
        with _db() as conn:
            m = conn.execute("SELECT COUNT(1) AS c FROM messages").fetchone()
            me = conn.execute("SELECT COUNT(1) AS c FROM media").fetchone()
            f = conn.execute("SELECT COUNT(1) AS c FROM forwarded").fetchone()

            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM media")
            conn.execute("DELETE FROM forwarded")

            return (
                int(m["c"] if m else 0),
                int(me["c"] if me else 0),
                int(f["c"] if f else 0),
            )
    except Exception:
        logger.exception("Failed to clear cache")
        return (0, 0, 0)


def _clear_media_dir() -> int:
    removed = 0
    try:
        if not MEDIA_DIR.exists():
            return 0
        for p in MEDIA_DIR.iterdir():
            try:
                if p.is_file():
                    p.unlink(missing_ok=True)
                    removed += 1
            except Exception:
                logger.exception("Failed to remove file %s", str(p))
    except Exception:
        logger.exception("Failed to clear media dir")
    return removed


def _db_forwarded_get(chat_id: int, message_id: int, tag: str) -> bool:
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT 1 FROM forwarded WHERE chat_id=? AND message_id=? AND tag=?",
                (int(chat_id), int(message_id), str(tag)),
            ).fetchone()
            return bool(row)
    except Exception:
        logger.exception("Failed to get forwarded flag")
        return False


def _db_forwarded_set(chat_id: int, message_id: int, tag: str) -> None:
    try:
        now = int(datetime.utcnow().timestamp())
        with _db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO forwarded(chat_id, message_id, tag, created_at) VALUES(?,?,?,?)",
                (int(chat_id), int(message_id), str(tag), now),
            )
    except Exception:
        logger.exception("Failed to set forwarded flag")


_db_init()
OWNER_ID = _db_get_owner_id()


def _db_kv_get(key: str) -> Optional[str]:
    try:
        with _db() as conn:
            row = conn.execute("SELECT value FROM kv WHERE key=?", (str(key),)).fetchone()
            return str(row["value"]) if row else None
    except Exception:
        logger.exception("Failed to get kv")
        return None


def _db_kv_set(key: str, value: str) -> None:
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(key), str(value)),
            )
    except Exception:
        logger.exception("Failed to set kv")


def _stars_price(plan: str) -> int:
    v = _db_kv_get(f"stars_price_{plan}")
    if v is not None:
        try:
            p = int(str(v).strip())
            if p > 0:
                return p
        except Exception:
            pass
    return int(DEFAULT_STARS_PRICES.get(plan, 0))


def _stars_plan(plan: str) -> Optional[tuple[int, int]]:
    if plan not in STARS_DURATIONS:
        return None
    price = _stars_price(plan)
    if price <= 0:
        return None
    return (price, int(STARS_DURATIONS[plan]))


def _stars_buttons() -> InlineKeyboardMarkup:
    p7 = _stars_price("7d")
    p14 = _stars_price("14d")
    p30 = _stars_price("30d")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"7 дней — {p7}⭐", callback_data="buy:7d"),
                InlineKeyboardButton(text=f"14 дней — {p14}⭐", callback_data="buy:14d"),
            ],
            [InlineKeyboardButton(text=f"30 дней — {p30}⭐", callback_data="buy:30d")],
        ]
    )


def _user_link(u: dict) -> str:
    user_id = u.get("id")
    username = u.get("username")

    if username:
        safe_user = html.escape(f"@{username}")
        return f"<a href=\"https://t.me/{html.escape(username)}\">{safe_user}</a>"

    name = " ".join([p for p in [u.get("first_name"), u.get("last_name")] if p]).strip()
    label = name or "Пользователь"
    safe_label = html.escape(label)

    if user_id is None:
        return safe_label

    return f"<a href=\"tg://user?id={int(user_id)}\">{safe_label}</a>"


def _msg_text(m: Any) -> str:
    if isinstance(m, dict):
        text = m.get("text") or m.get("caption")
        if text:
            return str(text)

        if m.get("photo") is not None:
            return "[photo]"
        if m.get("video") is not None:
            return "[video]"
        if m.get("voice") is not None:
            return "[voice message]"
        if m.get("video_note") is not None:
            return "[video note]"
        if m.get("sticker") is not None:
            return "[sticker]"
        if m.get("animation") is not None:
            return "[animation]"
        if m.get("audio") is not None:
            return "[audio]"
        if m.get("contact") is not None:
            return "[contact]"
        if m.get("location") is not None:
            return "[location]"
        if m.get("poll") is not None:
            q = (m.get("poll") or {}).get("question")
            return f"[poll] {q}" if q else "[poll]"
        if m.get("document") is not None:
            doc = m.get("document") or {}
            fname = doc.get("file_name")
            return f"[document] {fname}" if fname else "[document]"

        return "[content]"
    # aiogram Message
    return getattr(m, "text", None) or getattr(m, "caption", None) or "[content]"


async def _cache_media(*, chat_id: int, message_id: int, m: dict) -> None:
    if not isinstance(m, dict):
        return
    if m.get("_media_path"):
        return

    kind: Optional[str] = None
    file_id: Optional[str] = None

    if m.get("photo"):
        kind = "photo"
        try:
            file_id = (m.get("photo") or [])[-1].get("file_id")
        except Exception:
            file_id = None
    elif m.get("video"):
        kind = "video"
        file_id = (m.get("video") or {}).get("file_id")
    elif m.get("video_note"):
        kind = "video_note"
        file_id = (m.get("video_note") or {}).get("file_id")
    elif m.get("document"):
        kind = "document"
        file_id = (m.get("document") or {}).get("file_id")

    if not file_id or not kind:
        return

    try:
        f = await bot.get_file(file_id)
        unique = getattr(f, "file_unique_id", None) or file_id
        path = MEDIA_DIR / f"{datetime.utcnow().timestamp()}_{unique}"
        await bot.download_file(f.file_path, destination=path)
        m["_media_kind"] = kind
        m["_media_path"] = str(path)
        _db_set_media(chat_id, message_id, str(kind), str(path))
    except Exception:
        logger.exception("Failed to cache media")


async def _send_media_to_owner(
    *,
    owner_id: int,
    sender: dict,
    chat_label: str,
    media_kind: str,
    media_path: str,
    note: str,
    cleanup: bool,
) -> None:
    user_id = sender.get("id")
    username = sender.get("username")
    name = " ".join([p for p in [sender.get("first_name"), sender.get("last_name")] if p]).strip()
    label = f"@{username}" if username else (name or "Пользователь")

    header = (
        "⏳ Медиа с таймером сохранено\n"
        f"👤 Пользователь: {label}\n"
        f"💬 {chat_label}"
        + ("\n\n" + note if note else "")
    )

    def _offset(sub: str) -> int:
        i = header.find(sub)
        return _utf16_len(header[:i])

    entities: list[MessageEntity] = []
    if user_id:
        entities.append(
            MessageEntity(
                type="text_mention",
                offset=_offset(label),
                length=_utf16_len(label),
                user=User(id=int(user_id), is_bot=False, first_name=(name or label)),
            )
        )
    title = "Медиа с таймером сохранено"
    entities.append(MessageEntity(type="bold", offset=_offset(title), length=_utf16_len(title)))

    file = FSInputFile(media_path)
    try:
        if _is_blocked_effective(int(owner_id)):
            return
        if not _db_is_bot_user(int(owner_id)):
            return
        if media_kind == "photo":
            await bot.send_photo(owner_id, file)
        elif media_kind == "video":
            await bot.send_video(owner_id, file)
        elif media_kind == "video_note":
            await bot.send_video_note(owner_id, file)
        else:
            await bot.send_document(owner_id, file)

        await bot.send_message(owner_id, header, entities=entities, parse_mode=None)
    except TelegramForbiddenError:
        return
    finally:
        if cleanup:
            _unlink_quiet(media_path)


def _unlink_quiet(path: str) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        logger.exception("Failed to remove cached file %s", path)


def _enforce_chat_cache_limits(chat_id: int) -> None:
    _db_trim_chat(chat_id)


def _enforce_media_limit() -> None:
    _db_trim_media()


def _fmt_block(text: str, *, limit: int = 1500) -> str:
    t = (text or "").strip()
    if not t:
        t = "[empty]"
    if len(t) > limit:
        t = t[: limit - 3] + "..."
    return f"<pre>{html.escape(t)}</pre>"


def _chat_label(m: dict) -> str:
    chat = m.get("chat") or {}
    title = chat.get("title")
    if title:
        return html.escape(str(title))
    t = chat.get("type")
    cid = chat.get("id")
    if t == "private":
        return "Личный чат"
    if cid is not None:
        return f"Чат <code>{int(cid)}</code>"
    return "Чат"


def _build_edit_notify(*, user: dict, chat_label: str, old_text: str, new_text: str) -> str:
    return (
        "✏️ <b>Сообщение изменено</b>\n"
        f"👤 {_user_link(user)}\n"
        f"💬 {chat_label}\n\n"
        "🕓 <b>Было</b>:\n"
        f"{_fmt_block(old_text)}\n"
        "🆕 <b>Стало</b>:\n"
        f"{_fmt_block(new_text)}"
    )


def _build_diff_only(*, old_text: str, new_text: str) -> str:
    return (
        "🕓 <b>Было</b>:\n"
        f"{_fmt_block(old_text)}\n"
        "🆕 <b>Стало</b>:\n"
        f"{_fmt_block(new_text)}"
    )


def _utf16_len(s: str) -> int:
    # Telegram entity offsets are in UTF-16 code units
    return len(s.encode("utf-16-le")) // 2


async def _send_premium_header(chat_id: int, title: str) -> None:
    try:
        text = f"⭐ {title}"
        entities = [
            MessageEntity(
                type="custom_emoji",
                offset=0,
                length=_utf16_len("⭐"),
                custom_emoji_id=str(PREMIUM_CUSTOM_EMOJI_ID),
            )
        ]
        await bot.send_message(chat_id, text, entities=entities, parse_mode=None)
    except Exception:
        # Fallback to normal emoji if custom emoji can't be delivered
        await bot.send_message(chat_id, f"🌟 {title}")


async def _notify_deleted(*, owner_id: int, user: dict, chat_label: str, text_value: str) -> None:
    user_id = user.get("id")
    username = user.get("username")

    name = " ".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip()
    label = f"@{username}" if username else (name or "Пользователь")

    t = (text_value or "").strip() or "[empty]"
    if len(t) > 1500:
        t = t[:1497] + "..."

    msg = (
        "🗑 Сообщение удалено\n"
        f"👤 Пользователь: {label}\n"
        f"💬 {chat_label}\n\n"
        f"{t}"
    )

    def _offset(sub: str) -> int:
        i = msg.find(sub)
        return _utf16_len(msg[:i])

    entities: list[MessageEntity] = []

    if user_id:
        entities.append(
            MessageEntity(
                type="text_mention",
                offset=_offset(label),
                length=_utf16_len(label),
                user=User(id=int(user_id), is_bot=False, first_name=(name or label)),
            )
        )

    title = "Сообщение удалено"
    entities.append(MessageEntity(type="bold", offset=_offset(title), length=_utf16_len(title)))
    if t:
        entities.append(MessageEntity(type="italic", offset=_offset(t), length=_utf16_len(t)))

    try:
        if _is_blocked_effective(int(owner_id)):
            return
        if not _db_is_bot_user(int(owner_id)):
            return
        await bot.send_message(owner_id, msg, entities=entities, parse_mode=None)
    except TelegramForbiddenError:
        return


async def _notify_deleted_media(
    *,
    owner_id: int,
    user: dict,
    chat_label: str,
    text_value: str,
    media_kind: str,
    media_path: str,
    cleanup: bool,
) -> None:
    user_id = user.get("id")
    username = user.get("username")
    name = " ".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip()
    label = f"@{username}" if username else (name or "Пользователь")

    t = (text_value or "").strip() or "[empty]"
    if len(t) > 900:
        t = t[:897] + "..."

    # For video notes we don't want to show placeholder content like "[video note]"
    if media_kind == "video_note" and t.lower() in {"[video note]", "[content]", "[empty]"}:
        t = ""

    caption = (
        "🗑 Сообщение удалено\n"
        f"👤 Пользователь: {label}\n"
        f"💬 {chat_label}"
        + ("\n\n" + t if t else "")
    )

    def _offset(sub: str) -> int:
        i = caption.find(sub)
        return _utf16_len(caption[:i])

    entities: list[MessageEntity] = []
    if user_id:
        entities.append(
            MessageEntity(
                type="text_mention",
                offset=_offset(label),
                length=_utf16_len(label),
                user=User(id=int(user_id), is_bot=False, first_name=(name or label)),
            )
        )

    title = "Сообщение удалено"
    entities.append(MessageEntity(type="bold", offset=_offset(title), length=_utf16_len(title)))
    entities.append(MessageEntity(type="italic", offset=_offset(t), length=_utf16_len(t)))

    file = FSInputFile(media_path)
    try:
        if _is_blocked_effective(int(owner_id)):
            return
        if not _db_is_bot_user(int(owner_id)):
            return
        if media_kind == "photo":
            await bot.send_photo(owner_id, file, caption=caption, caption_entities=entities, parse_mode=None)
        elif media_kind == "video":
            await bot.send_video(owner_id, file, caption=caption, caption_entities=entities, parse_mode=None)
        elif media_kind == "video_note":
            # video_note doesn't support captions in Bot API, so send note first, then a text with details
            await bot.send_video_note(owner_id, file)
            await bot.send_message(owner_id, caption, entities=entities, parse_mode=None)
        else:
            await bot.send_document(owner_id, file, caption=caption, caption_entities=entities, parse_mode=None)
    except Exception:
        logger.exception("Failed to send deleted media")
        try:
            await bot.send_message(owner_id, caption, entities=entities, parse_mode=None)
        except TelegramForbiddenError:
            pass
    finally:
        if cleanup:
            # Free disk space
            _unlink_quiet(media_path)
        # DB row is removed when processing deleted_business_messages


async def _notify_edit(*, owner_id: int, user: dict, chat_label: str, old_text: str, new_text: str) -> None:
    """Send notification about an edit.

    Uses entities (text_mention + bold + pre) so the profile is clickable and layout is clean.
    """
    user_id = user.get("id")
    username = user.get("username")

    name = " ".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip()
    label = f"@{username}" if username else (name or "Пользователь")

    old_t = (old_text or "").strip() or "[empty]"
    new_t = (new_text or "").strip() or "[empty]"
    if len(old_t) > 1500:
        old_t = old_t[:1497] + "..."
    if len(new_t) > 1500:
        new_t = new_t[:1497] + "..."

    text = (
        "✏️ Сообщение изменено\n"
        f"👤 Пользователь: {label}\n"
        f"💬 {chat_label}\n\n"
        "🕓 Было:\n"
        f"{old_t}\n\n"
        "🆕 Стало:\n"
        f"{new_t}"
    )

    def _offset(sub: str) -> int:
        i = text.find(sub)
        return _utf16_len(text[:i])

    entities: list[MessageEntity] = []

    # Make label clickable
    if user_id:
        start = _offset(label)
        entities.append(
            MessageEntity(
                type="text_mention",
                offset=start,
                length=_utf16_len(label),
                user=User(id=int(user_id), is_bot=False, first_name=(name or label)),
            )
        )

    # Bold only the main title
    title = "Сообщение изменено"
    entities.append(MessageEntity(type="bold", offset=_offset(title), length=_utf16_len(title)))

    # Make the old text italic; new text stays plain (no formatting)
    old_start = _offset(old_t)
    entities.append(MessageEntity(type="italic", offset=old_start, length=_utf16_len(old_t)))

    try:
        if _is_blocked_effective(int(owner_id)):
            return
        if not _db_is_bot_user(int(owner_id)):
            return
        await bot.send_message(owner_id, text, entities=entities, parse_mode=None)
    except TelegramForbiddenError:
        return


async def _send_start(chat_id: int, *, set_owner: bool) -> None:
    global OWNER_ID
    if set_owner:
        OWNER_ID = chat_id
        _db_set_owner_id(chat_id)
    bot_username = await _get_bot_username()

    is_admin = int(chat_id) == int(ADMIN_ID)
    menu = _main_menu_kb(is_admin=is_admin)
    await _send_premium_header(chat_id, "Добро пожаловать!")
    text = (
        "🕵️‍♂️ Этот бот помогает следить за вашими бизнес-чатами.\n\n"
        "Что он умеет:\n"
        "• Уведомляет об изменениях сообщений ✏️\n"
        "• Уведомляет об удалениях 🗑\n"
        "• Сохраняет исчезающие фото/видео при ответе ⏳\n\n"
        "Подключение: Telegram → Telegram для бизнеса → Чат-боты → добавить:\n"
        f"<code>@{bot_username}</code>"
    )
    await bot.send_message(chat_id, text, reply_markup=menu)
    my_connections = _db_count_business_connections_for_owner(int(chat_id))
    my_chats = _db_owner_chat_count(int(chat_id))
    if my_connections > 0:
        await bot.send_message(
            chat_id,
            f"✅ Бот подключён в Telegram для бизнеса. Подключений: {my_connections}. Чатов: {my_chats}.",
        )
    else:
        await bot.send_message(
            chat_id,
            f"❗️ Пока нет подключённых бизнес-чатов. Добавьте @{bot_username} в Telegram для бизнеса → Чат-боты и начните переписку (вы можете написать кому-то сами).",
        )


async def _process_update(update: dict) -> None:
    # Minimal debug for business routing (helps verify business_connection_id is present)
    if any(k in update for k in ("business_connection", "business_message", "edited_business_message", "deleted_business_messages")):
        try:
            logger.info("BUSINESS UPDATE KEYS %s", [k for k in update.keys() if "business" in k])
        except Exception:
            pass

    # Telegram Stars payments
    if "pre_checkout_query" in update:
        q = update.get("pre_checkout_query") or {}
        qid = q.get("id")
        ok = True
        err = None
        payload = (q.get("invoice_payload") or "").strip()
        currency = q.get("currency")
        if currency and str(currency) != str(STARS_CURRENCY):
            ok = False
            err = "Unsupported currency"
        if not payload.startswith("sub:"):
            ok = False
            err = "Invalid payload"
        try:
            if qid:
                await bot.answer_pre_checkout_query(str(qid), ok=bool(ok), error_message=err)
        except Exception:
            logger.exception("Failed to answer pre_checkout_query")
        return

    # successful_payment comes as a normal message
    if "message" in update and isinstance(update.get("message"), dict):
        m0 = update.get("message") or {}
        sp = m0.get("successful_payment")
        if isinstance(sp, dict):
            chat = (m0.get("chat") or {}) if isinstance(m0.get("chat"), dict) else {}
            uid = chat.get("id")
            payload = (sp.get("invoice_payload") or "").strip()
            if uid is not None and payload.startswith("sub:"):
                parts = payload.split(":")
                plan = parts[1] if len(parts) > 1 else ""
                p = _stars_plan(plan)
                if p:
                    _, seconds = p
                    until = _db_sub_extend(int(uid), int(seconds))
                    try:
                        await bot.send_message(int(uid), f"✅ Оплата получена. Доступ активен до {_fmt_dt(until)}")
                    except Exception:
                        logger.exception("Failed to confirm payment")
            return

    # TEMP debug: dump JSON for edited_business_message/business_message once per update_id
    global _DEBUG_LAST_BUSINESS_UPDATE_ID
    upd_id = update.get("update_id")
    if (
        isinstance(upd_id, int)
        and upd_id != _DEBUG_LAST_BUSINESS_UPDATE_ID
        and ("edited_business_message" in update or "business_message" in update)
    ):
        _DEBUG_LAST_BUSINESS_UPDATE_ID = upd_id
        try:
            s = json.dumps(update, ensure_ascii=False)
            if len(s) > 3500:
                s = s[:3500] + "..."
            logger.info("BUSINESS UPDATE JSON %s", s)
        except Exception:
            logger.exception("Failed to dump business update JSON")

    # Business connection mapping
    if "business_connection" in update:
        bc = update.get("business_connection") or {}
        if isinstance(bc, dict):
            cid = _extract_business_connection_id(bc)
            user = bc.get("user") if isinstance(bc.get("user"), dict) else None
            owner_uid = None
            if user and user.get("id"):
                owner_uid = int(user.get("id"))
            elif bc.get("user_id") is not None:
                owner_uid = int(bc.get("user_id"))
            if cid and owner_uid:
                # Persist mapping for allowed owners even if they haven't started the bot yet.
                # business_connection is authoritative for ownership (comes from Telegram).
                if _db_is_whitelisted(int(owner_uid)):
                    prev_owner = _db_get_owner_by_connection(str(cid))
                    _db_set_business_connection(cid, owner_uid)
                    if prev_owner != int(owner_uid):
                        logger.info("Mapped business_connection %s: %s -> %s", cid, prev_owner, owner_uid)
                # Notify owner immediately when bot is added to Telegram Business (once per connection)
                # Only if bot can PM the owner.
                if _db_mark_connection_notified(cid) and _db_is_bot_user(int(owner_uid)):
                    try:
                        await bot.send_message(
                            owner_uid,
                            "✅ Бот добавлен в Telegram для бизнеса и готов работать.\n"
                            "Теперь он будет присылать уведомления по вашим бизнес-чатам.",
                        )
                    except Exception:
                        logger.exception("Failed to notify owner about business connection")
        return
    # Button callbacks (inline UX)
    if "callback_query" in update:
        cq = update.get("callback_query") or {}
        cq_id = cq.get("id")
        data = cq.get("data")
        from_user = cq.get("from") or {}
        from_id = from_user.get("id")

        if from_id and _is_blocked_effective(int(from_id)):
            try:
                if cq_id:
                    await bot.answer_callback_query(str(cq_id), text="🚫 Вы заблокированы", show_alert=True)
            except Exception:
                pass
            return

        if isinstance(from_user, dict):
            _db_touch_user(from_user)
        if from_id:
            _db_mark_bot_user(int(from_id))

        # Do not track keyboard button presses (privacy-friendly)

        try:
            if cq_id:
                await bot.answer_callback_query(str(cq_id))
        except Exception:
            logger.exception("Failed to answer callback_query")

        if not from_id:
            return

        if isinstance(data, str) and data.startswith("buy:"):
            plan = data.split(":", 1)[1]
            p = _stars_plan(plan)
            if not p:
                await bot.send_message(int(from_id), "❌ Неизвестный тариф")
                return
            if _is_blocked_effective(int(from_id)):
                await bot.send_message(int(from_id), "🚫 Вы заблокированы")
                return
            stars, _ = p
            payload = f"sub:{plan}:{int(from_id)}"
            title = "Доступ к бизнес-уведомлениям"
            descr = {
                "7d": "Доступ на 7 дней",
                "14d": "Доступ на 14 дней",
                "30d": "Доступ на 30 дней",
            }.get(plan, "Доступ")
            try:
                await bot.send_invoice(
                    chat_id=int(from_id),
                    title=title,
                    description=descr,
                    payload=payload,
                    provider_token="",
                    currency=str(STARS_CURRENCY),
                    prices=[LabeledPrice(label=descr, amount=int(stars))],
                )
            except Exception:
                logger.exception("Failed to send Stars invoice")
                await bot.send_message(int(from_id), "❌ Не удалось создать счёт. Попробуйте позже.")
            return

        if data == "admin_prices" and int(from_id) == int(ADMIN_ID):
            p7 = _stars_price("7d")
            p14 = _stars_price("14d")
            p30 = _stars_price("30d")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✏️ Изменить", callback_data="admin_prices_edit")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
                ]
            )
            await bot.send_message(
                int(from_id),
                "⭐ Цены Stars\n\n"
                f"7 дней: {p7}⭐\n"
                f"14 дней: {p14}⭐\n"
                f"30 дней: {p30}⭐\n\n"
                "Нажми ✏️ Изменить, затем отправь одним сообщением: 7=15 14=25 30=45",
                reply_markup=kb,
            )
            return

        if data == "admin_prices_edit" and int(from_id) == int(ADMIN_ID):
            _db_access_set_pending(int(from_id), True, "prices")
            await bot.send_message(int(from_id), "✏️ Отправь новые цены: 7=15 14=25 30=45")
            return

        if data == "help":
            await _send_help(int(from_id))
            return

        if data == "status":
            await _send_status(int(from_id))
            return

        if data == "privacy":
            await _send_privacy(int(from_id))
            return

        if data == "support":
            _db_support_set_pending(int(from_id), True)
            await _send_support_prompt(int(from_id))
            return

        if data == "support_cancel":
            _db_support_set_pending(int(from_id), False)
            await bot.send_message(int(from_id), "Отменено.")
            return

        if data == "admin" and int(from_id) == int(ADMIN_ID):
            total_users = _db_stats_users_count()
            users = _db_list_users(limit=8, offset=0)

            paid_mode = _db_get_paid_mode()

            lines = ["👑 Админка", "", f"Пользователей: {total_users}", "", "Пользователи:"]
            kb_rows: list[list[InlineKeyboardButton]] = []
            entities: list[MessageEntity] = []
            text_so_far = ""
            if users:
                for u in users:
                    uid = int(u["user_id"])
                    uname = u["username"]
                    name = u["name"]
                    who = (f"@{uname}" if uname else (name or f"Пользователь {uid}")).strip()
                    blocked = bool(u["blocked"])

                    line = f"- {uid} — {who}" + (" — 🚫 блок" if blocked else "")
                    # compute entity offsets based on the final joined text
                    # current text is lines + this line joined with \n
                    preview = "\n".join(lines + [line])
                    start = preview.rfind(who)
                    if start >= 0:
                        entities.append(
                            MessageEntity(
                                type="text_mention",
                                offset=_utf16_len(preview[:start]),
                                length=_utf16_len(who),
                                user=User(id=uid, is_bot=False, first_name=(name or (uname or str(uid)))),
                            )
                        )

                    lines.append(line)
                    kb_rows.append(
                        [
                            InlineKeyboardButton(text=f"⚙️ {uid}", callback_data=f"admin_u:{uid}"),
                        ]
                    )
            else:
                lines.append("- пока нет пользователей")

            kb_rows.insert(
                0,
                [
                    InlineKeyboardButton(
                        text=("💰 Платный режим: ВКЛ" if paid_mode else "💸 Платный режим: ВЫКЛ"),
                        callback_data="admin_paid_toggle",
                    ),
                    InlineKeyboardButton(text="🎁 Бесплатные", callback_data="admin_free_list"),
                ],
            )
            kb_rows.insert(1, [InlineKeyboardButton(text="⭐ Цены", callback_data="admin_prices")])

            kb_rows.append([InlineKeyboardButton(text="🚫 Черный список", callback_data="admin_blacklist")])
            kb_rows.append([InlineKeyboardButton(text="🧹 Очистить кэш", callback_data="admin_cache_clear")])

            kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
            await bot.send_message(int(from_id), "\n".join(lines), reply_markup=kb, entities=entities, parse_mode=None)
            return

        if data == "admin_paid_toggle" and int(from_id) == int(ADMIN_ID):
            cur = _db_get_paid_mode()
            _db_set_paid_mode(not cur)
            await bot.send_message(int(from_id), f"✅ Платный режим теперь: {'ВКЛ' if not cur else 'ВЫКЛ'}")
            update2 = {"callback_query": {"id": "0", "data": "admin", "from": from_user}}
            await _process_update(update2)
            return

        if data == "admin_free_list" and int(from_id) == int(ADMIN_ID):
            ids = _db_list_free_users(limit=50)
            lines = ["🎁 Бесплатные пользователи", ""]
            if not ids:
                lines.append("- список пуст")
            else:
                for uid in ids:
                    lines.append(f"- {uid}")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")]]
            )
            await bot.send_message(int(from_id), "\n".join(lines), reply_markup=kb)
            return

        if data == "admin_cache_clear" and int(from_id) == int(ADMIN_ID):
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, очистить", callback_data="admin_cache_clear_yes"),
                        InlineKeyboardButton(text="❌ Нет", callback_data="admin_cache_clear_no"),
                    ],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
                ]
            )
            await bot.send_message(
                int(from_id),
                "🧹 Очистка кэша\n\nУдалить кэш сообщений/медиа и файлы из папки media?",
                reply_markup=kb,
            )
            return

        if data == "admin_cache_clear_no" and int(from_id) == int(ADMIN_ID):
            await bot.send_message(int(from_id), "Отменено.")
            return

        if data == "admin_cache_clear_yes" and int(from_id) == int(ADMIN_ID):
            removed_files = _clear_media_dir()
            m_cnt, me_cnt, f_cnt = _db_clear_cache()
            await bot.send_message(
                int(from_id),
                f"✅ Кэш очищен.\n\nmessages: {m_cnt}\nmedia rows: {me_cnt}\nforwarded: {f_cnt}\nfiles deleted: {removed_files}",
            )
            return

        if data == "admin_blacklist" and int(from_id) == int(ADMIN_ID):
            users = _db_list_blocked_users(limit=25, offset=0)
            lines = ["🚫 Черный список", "", "Заблокированные пользователи:"]
            entities: list[MessageEntity] = []
            kb_rows: list[list[InlineKeyboardButton]] = []
            if not users:
                lines.append("- список пуст")
            else:
                for u in users:
                    uid = int(u["user_id"])
                    uname = u["username"]
                    name = u["name"]
                    who = (f"@{uname}" if uname else (name or f"Пользователь {uid}")).strip()

                    line = f"- {uid} — {who}"
                    preview = "\n".join(lines + [line])
                    start = preview.rfind(who)
                    if start >= 0:
                        entities.append(
                            MessageEntity(
                                type="text_mention",
                                offset=_utf16_len(preview[:start]),
                                length=_utf16_len(who),
                                user=User(id=uid, is_bot=False, first_name=(name or (uname or str(uid)))),
                            )
                        )
                    lines.append(line)
                    kb_rows.append([InlineKeyboardButton(text=f"✅ Разблокировать {uid}", callback_data=f"admin_unblock:{uid}")])

            kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")])
            kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
            text = "\n".join(lines)
            if len(text) > 3800:
                text = text[:3800] + "..."
            await bot.send_message(int(from_id), text, reply_markup=kb, entities=entities, parse_mode=None)
            return

        if isinstance(data, str) and data.startswith("admin_u:") and int(from_id) == int(ADMIN_ID):
            try:
                uid = int(data.split(":", 1)[1])
            except Exception:
                return
            blocked = _db_is_blocked(uid)
            is_free = _db_is_free_user(uid)
            who = str(uid)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=("✅ Разблокировать" if blocked else "🚫 Заблокировать"),
                            callback_data=(f"admin_unblock:{uid}" if blocked else f"admin_block:{uid}"),
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text=("✅ В whitelist" if not _db_is_whitelisted(uid) else "❌ Убрать из whitelist"),
                            callback_data=(f"admin_wl_add:{uid}" if not _db_is_whitelisted(uid) else f"admin_wl_del:{uid}"),
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text=("🎁 Сделать бесплатным" if not is_free else "💰 Убрать бесплатный"),
                            callback_data=(f"admin_free_add:{uid}" if not is_free else f"admin_free_del:{uid}"),
                        )
                    ],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
                    [InlineKeyboardButton(text="🚫 Черный список", callback_data="admin_blacklist")],
                ]
            )
            await bot.send_message(int(from_id), f"Пользователь: {who}", reply_markup=kb)
            return

        if isinstance(data, str) and data.startswith("admin_free_add:") and int(from_id) == int(ADMIN_ID):
            try:
                uid = int(data.split(":", 1)[1])
            except Exception:
                return
            _db_set_free_user(uid, True)
            await bot.send_message(int(from_id), f"🎁 Пользователь {uid} теперь бесплатный")
            update2 = {"callback_query": {"id": "0", "data": f"admin_u:{uid}", "from": from_user}}
            await _process_update(update2)
            return

        if isinstance(data, str) and data.startswith("admin_free_del:") and int(from_id) == int(ADMIN_ID):
            try:
                uid = int(data.split(":", 1)[1])
            except Exception:
                return
            _db_set_free_user(uid, False)
            await bot.send_message(int(from_id), f"💰 Пользователь {uid} убран из бесплатных")
            update2 = {"callback_query": {"id": "0", "data": f"admin_u:{uid}", "from": from_user}}
            await _process_update(update2)
            return

        if isinstance(data, str) and data.startswith("admin_wl_add:") and int(from_id) == int(ADMIN_ID):
            try:
                uid = int(data.split(":", 1)[1])
            except Exception:
                return
            _db_set_whitelisted(uid, True)
            await bot.send_message(int(from_id), f"✅ Пользователь {uid} добавлен в whitelist")
            return

        if isinstance(data, str) and data.startswith("admin_wl_del:") and int(from_id) == int(ADMIN_ID):
            try:
                uid = int(data.split(":", 1)[1])
            except Exception:
                return
            _db_set_whitelisted(uid, False)
            await bot.send_message(int(from_id), f"❌ Пользователь {uid} убран из whitelist")
            return

        if isinstance(data, str) and data.startswith("admin_block:") and int(from_id) == int(ADMIN_ID):
            try:
                uid = int(data.split(":", 1)[1])
            except Exception:
                return
            _db_set_blocked(uid, True)
            await bot.send_message(int(from_id), f"🚫 Пользователь {uid} заблокирован")
            return

        if isinstance(data, str) and data.startswith("admin_unblock:") and int(from_id) == int(ADMIN_ID):
            try:
                uid = int(data.split(":", 1)[1])
            except Exception:
                return
            _db_set_blocked(uid, False)
            await bot.send_message(int(from_id), f"✅ Пользователь {uid} разблокирован")
            return

        return

    # Cache originals
    for key in ("message", "business_message"):
        if key not in update:
            continue

        m = update[key]
        sender = m.get("from") or {}

        if key == "message":
            try:
                chat_id_b = (m.get("chat") or {}).get("id")
                if chat_id_b is not None and _is_blocked_effective(int(chat_id_b)):
                    return
            except Exception:
                pass
        if key == "message" and isinstance(sender, dict):
            # Track only real bot users (private chat) to reduce DB writes
            _db_touch_user(sender)

        # Business connection binding & per-owner chat tracking
        if key == "business_message":
            cid = _extract_business_connection_id(m)

            # Ownership recovery: if this is an outgoing business message, the sender is the business owner.
            # For incoming messages from the external participant, from.id == chat.id.
            # For outgoing messages, from.id != chat.id.
            try:
                chat_obj = m.get("chat") or {}
                chat_id_b = chat_obj.get("id")
                sender_obj = m.get("from") or {}
                sender_id_b = sender_obj.get("id")
                if cid and chat_id_b is not None and sender_id_b is not None:
                    s_id = int(sender_id_b)
                    c_id = int(chat_id_b)
                    if s_id != c_id and _db_is_bot_user(s_id) and _db_is_whitelisted(s_id):
                        prev_owner = _db_get_owner_by_connection(str(cid))
                        if prev_owner != int(s_id):
                            _db_set_business_connection(str(cid), int(s_id))
                            logger.info("Recovered business_connection owner %s: %s -> %s", str(cid), prev_owner, int(s_id))
            except Exception:
                logger.exception("Failed to recover business_connection owner")

            # Bind only when the owner explicitly requested binding (prevents cross-user leakage).
            # Important: business_message.sender can be the external chat participant.
            if cid:
                pending_owner = _db_business_bind_pick_pending_owner()
                if pending_owner is not None and _db_business_bind_is_pending(int(pending_owner)):
                    if _db_is_bot_user(int(pending_owner)) and _db_is_whitelisted(int(pending_owner)):
                        prev_owner = _db_get_owner_by_connection(str(cid))
                        if _db_upsert_business_connection_safe(str(cid), int(pending_owner)):
                            logger.info(
                                "Rebound business_connection %s: %s -> %s",
                                str(cid),
                                prev_owner,
                                int(pending_owner),
                            )
                    _db_business_bind_clear(int(pending_owner))
                    # Notify owner once (only if bot can PM)
                    if _db_is_bot_user(int(pending_owner)):
                        try:
                            await bot.send_message(
                                int(pending_owner),
                                "✅ Бизнес-аккаунт привязан. Теперь уведомления будут приходить только по вашим бизнес-чатам.",
                            )
                        except TelegramForbiddenError:
                            pass

            # Track chats only when mapping exists
            if cid:
                mapped_owner = _db_get_owner_by_connection(str(cid))
                chat_id_b = (m.get("chat") or {}).get("id")
                if mapped_owner and chat_id_b is not None:
                    _db_owner_chat_add(int(mapped_owner), int(chat_id_b))

        chat_id = m.get("chat", {}).get("id")
        msg_id = m.get("message_id")
        if chat_id is None or msg_id is None:
            continue

        # Count as "bot user" only if they actually interact with the bot in private chat
        if key == "message":
            chat = m.get("chat") or {}
            if chat.get("type") == "private":
                _db_mark_bot_user(int(chat_id))

                # Access-code gating: only whitelisted users can use features (except admin)
                if int(chat_id) != int(ADMIN_ID) and not _db_is_whitelisted(int(chat_id)):
                    pending_kind = _db_access_get_pending(int(chat_id))
                    txt_raw = (m.get("text") or "").strip()
                    if txt_raw == "/start":
                        _db_access_set_pending(int(chat_id), True, "user")
                        await bot.send_message(int(chat_id), "🔐 Введите код доступа")
                        return

                    if txt == "⭐ Купить доступ":
                        kb = _stars_buttons()
                        await _send_premium_header(int(chat_id), "Оплата Telegram Stars")
                        await bot.send_message(
                            int(chat_id),
                            "Выберите тариф. Оплата проходит в Telegram Stars.",
                            reply_markup=kb,
                        )
                        return

                    if pending_kind == "user":
                        code = _db_get_access_code()
                        if not code:
                            await bot.send_message(int(chat_id), "⛔️ Доступ закрыт.")
                            return
                        if txt_raw == code:
                            _db_access_set_pending(int(chat_id), False, "user")
                            _db_set_whitelisted(int(chat_id), True)
                            await bot.send_message(int(chat_id), "✅ Доступ открыт")
                            await _send_start(int(chat_id), set_owner=False)
                            return
                        await bot.send_message(int(chat_id), "❌ Неверный код")
                        return

                    await bot.send_message(int(chat_id), "🔐 Для доступа нажмите /start и введите код")
                    return

                # ReplyKeyboard menu commands
                txt = (m.get("text") or "").strip()
                if txt in {
                    "📌 Инструкция",
                    "📊 Статус",
                    "🔒 Конфиденциальность",
                    "🆘 Техподдержка",
                    "🔗 Подключить бизнес",
                    "⭐ Купить доступ",
                    "👑 Админка",
                    "🚫 Черный список",
                    "📣 Рассылка",
                    "❌ Отмена",
                    "🔑 Код доступа",
                }:
                    if txt == "📌 Инструкция":
                        await _send_help(int(chat_id))
                        return
                    if txt == "📊 Статус":
                        await _send_status(int(chat_id))
                        return
                    if txt == "🔒 Конфиденциальность":
                        await _send_privacy(int(chat_id))
                        return
                    if txt == "🆘 Техподдержка":
                        _db_support_set_pending(int(chat_id), True)
                        await _send_support_prompt(int(chat_id))
                        return

                    if txt == "🔗 Подключить бизнес":
                        if not _has_active_subscription(int(chat_id)):
                            kb = _stars_buttons()
                            await bot.send_message(
                                int(chat_id),
                                "⭐ Для подключения бизнеса нужен активный доступ. Выберите тариф:",
                                reply_markup=kb,
                            )
                            return
                        my_connections = _db_count_business_connections_for_owner(int(chat_id))
                        my_chats = _db_owner_chat_count(int(chat_id))
                        if my_connections > 0:
                            await bot.send_message(
                                int(chat_id),
                                f"✅ Уже подключено. Подключений: {my_connections}. Чатов: {my_chats}.",
                            )
                            return
                        _db_business_bind_set_pending(int(chat_id), True)
                        await _send_premium_header(int(chat_id), "Подключение Telegram для бизнеса")
                        await bot.send_message(
                            int(chat_id),
                            "1) Убедитесь, что бот добавлен: Telegram → Telegram для бизнеса → Чат-боты\n"
                            "2) Затем напишите любому человеку от имени бизнеса (или вам напишут)\n\n"
                            "После первого сообщения бот привяжет ваш бизнес-аккаунт и начнёт присылать уведомления только по вашим чатам.",
                        )
                        return
                    if txt == "📣 Рассылка" and int(chat_id) == int(ADMIN_ID):
                        _db_broadcast_set_pending(int(chat_id), True)
                        await _send_broadcast_prompt(int(chat_id))
                        return
                    if txt == "❌ Отмена":
                        _db_support_set_pending(int(chat_id), False)
                        _db_broadcast_set_pending(int(chat_id), False)
                        _db_access_set_pending(int(chat_id), False, "admin")
                        await bot.send_message(int(chat_id), "Отменено.")
                        return
                    if txt == "👑 Админка" and int(chat_id) == int(ADMIN_ID):
                        # emulate admin callback
                        update2 = {"callback_query": {"id": "0", "data": "admin", "from": sender}}
                        await _process_update(update2)
                        return
                    if txt == "🚫 Черный список" and int(chat_id) == int(ADMIN_ID):
                        update2 = {"callback_query": {"id": "0", "data": "admin_blacklist", "from": sender}}
                        await _process_update(update2)
                        return

                    if txt == "🔑 Код доступа" and int(chat_id) == int(ADMIN_ID):
                        cur = _db_get_access_code()
                        await bot.send_message(
                            int(chat_id),
                            f"🔑 Текущий код: {cur if cur else '[не задан]'}\n\nОтправь новый код одним сообщением.",
                        )
                        _db_access_set_pending(int(chat_id), True, "admin")
                        return

                # Admin: set access code by next message
                if int(chat_id) == int(ADMIN_ID) and _db_access_get_pending(int(chat_id)) == "prices":
                    txt_raw = (m.get("text") or "").strip()
                    if txt_raw and txt_raw != "❌ Отмена":
                        vals: dict[str, int] = {}
                        parts = txt_raw.replace(",", " ").split()
                        for token in parts:
                            if "=" in token:
                                k, v = token.split("=", 1)
                            elif ":" in token:
                                k, v = token.split(":", 1)
                            else:
                                continue
                            k = k.strip()
                            v = v.strip()
                            if k in {"7", "7d"}:
                                vals["7d"] = int(v)
                            elif k in {"14", "14d"}:
                                vals["14d"] = int(v)
                            elif k in {"30", "30d"}:
                                vals["30d"] = int(v)
                        if vals:
                            for plan, price in vals.items():
                                if int(price) > 0:
                                    _db_kv_set(f"stars_price_{plan}", str(int(price)))
                            _db_access_set_pending(int(chat_id), False, "prices")
                            await bot.send_message(int(chat_id), "✅ Цены обновлены")
                            return
                        await bot.send_message(int(chat_id), "❌ Не понял формат. Пример: 7=15 14=25 30=45")
                        return

                if int(chat_id) == int(ADMIN_ID) and _db_access_get_pending(int(chat_id)) == "admin":
                    txt_raw = (m.get("text") or "").strip()
                    if txt_raw and txt_raw != "❌ Отмена":
                        _db_set_access_code(txt_raw)
                        _db_access_set_pending(int(chat_id), False, "admin")
                        await bot.send_message(int(chat_id), "✅ Код обновлён")
                        return

                # Broadcast flow (admin only): send next message to all users
                if int(chat_id) == int(ADMIN_ID) and _db_broadcast_is_pending(int(chat_id)):
                    _db_broadcast_set_pending(int(chat_id), False)
                    user_ids = _db_list_bot_user_ids()
                    ok = 0
                    fail = 0
                    for uid in user_ids:
                        if int(uid) == int(ADMIN_ID):
                            continue
                        try:
                            await bot.copy_message(chat_id=int(uid), from_chat_id=int(chat_id), message_id=int(msg_id))
                            ok += 1
                        except Exception:
                            fail += 1
                        await asyncio.sleep(0.05)
                    await bot.send_message(int(chat_id), f"✅ Рассылка завершена. Успешно: {ok}, ошибок: {fail}.")
                    return

                # Tech support flow: if user requested support, forward next message to support group
                if _db_support_is_pending(int(chat_id)):
                    _db_support_set_pending(int(chat_id), False)
                    u = sender if isinstance(sender, dict) else {}
                    uid = int(u.get("id") or int(chat_id))
                    uname = u.get("username")
                    name = " ".join([p for p in [u.get("first_name"), u.get("last_name")] if p]).strip()
                    who = (f"@{uname}" if uname else (name or f"Пользователь {uid}")).strip()

                    complaint = _msg_text(m)
                    text = f"🆘 Обращение в поддержку\n👤 {who} (id {uid})\n\n{complaint}"
                    entities: list[MessageEntity] = []
                    start = text.find(who)
                    if start >= 0:
                        entities.append(
                            MessageEntity(
                                type="text_mention",
                                offset=_utf16_len(text[:start]),
                                length=_utf16_len(who),
                                user=User(id=uid, is_bot=False, first_name=(name or (uname or str(uid)))),
                            )
                        )
                    try:
                        await bot.send_message(SUPPORT_CHAT_ID, text, entities=entities, parse_mode=None)
                    except Exception:
                        logger.exception("Failed to send support message")

                    await bot.send_message(int(chat_id), "✅ Спасибо! Сообщение отправлено в поддержку.")
                    return

        # Bind OWNER_ID only for the main admin
        global OWNER_ID
        if OWNER_ID is None and key == "message":
            chat = m.get("chat") or {}
            if chat.get("type") == "private":
                if int(chat_id) == int(ADMIN_ID):
                    OWNER_ID = int(chat_id)
                    _db_set_owner_id(OWNER_ID)
                    logger.info("Owner registered via private chat: %s", OWNER_ID)

        connected_chats.add(int(chat_id))
        _db_put_message(int(chat_id), int(msg_id), m)

        await _cache_media(chat_id=int(chat_id), message_id=int(msg_id), m=m)

        # If you reply to a disappearing/timer photo/video, Telegram often includes it in reply_to_message.
        # Cache that media too, so it can be saved even if it disappears later.
        r = m.get("reply_to_message")
        if isinstance(r, dict):
            r_msg_id = r.get("message_id")
            if r_msg_id is not None:
                _db_put_message(int(chat_id), int(r_msg_id), r)
                await _cache_media(chat_id=int(chat_id), message_id=int(r_msg_id), m=r)

                # If reply_to_message contains timer media, immediately forward it to owner
                # Privacy-safe: forward only for business messages with a resolved owner mapping.
                sender = (m.get("from") or {})
                owner_id = _target_owner_for_business(m)
                # If this update arrived in another bot user's stream, chat.id will be the other bot user.
                # In that case, forward to chat.id (the real owner of the chat being monitored), not to the saver.
                target_id = int(owner_id) if owner_id else None
                try:
                    if chat_id is not None and _db_is_bot_user(int(chat_id)):
                        target_id = int(chat_id)
                except Exception:
                    pass
                # Dedup by recipient (owner_id), not by chat_id, because Telegram can deliver the same reply
                # via different business streams with different chat_id values.
                if target_id and not _db_forwarded_get(int(target_id), int(r_msg_id), "ephemeral"):
                    media = _db_get_media(int(chat_id), int(r_msg_id))
                    if media and media.get("kind") and media.get("path"):
                        _db_forwarded_set(int(target_id), int(r_msg_id), "ephemeral")
                        logger.info(
                            "Forwarding timer media to owner: kind=%s path=%s",
                            media.get("kind"),
                            media.get("path"),
                        )
                        logger.info(
                            "Timer media routing: cid=%s owner_id=%s chat_id=%s target_id=%s",
                            _extract_business_connection_id(m),
                            owner_id,
                            chat_id,
                            target_id,
                        )
                        note = ""
                        # do not show placeholders like [photo]/[video note]
                        caption_or_text = r.get("caption")
                        if caption_or_text:
                            note = str(caption_or_text)
                        await _send_media_to_owner(
                            owner_id=int(target_id),
                            sender=sender,
                            chat_label=_chat_label(r),
                            media_kind=str(media.get("kind")),
                            media_path=str(media.get("path")),
                            note=note,
                            cleanup=True,
                        )

        _enforce_chat_cache_limits(int(chat_id))
        _enforce_media_limit()

        # /start handling
        if key == "message" and m.get("text") == "/start":
            if sender and isinstance(sender, dict) and sender.get("id"):
                _db_log_event(user_id=int(sender.get("id")), action="start")
            # Only the main admin can register/set the OWNER_ID.
            await _send_start(int(chat_id), set_owner=(int(chat_id) == int(ADMIN_ID)))

    # Business edit notification (main target)
    if "edited_business_message" in update:
        m = update["edited_business_message"]
        owner_id = _target_owner_for_business(m)
        editor = (m.get("from") or {}) if isinstance(m.get("from"), dict) else {}
        chat_id = m.get("chat", {}).get("id")
        msg_id = m.get("message_id")
        if chat_id is None or msg_id is None:
            return
        chat_id_i = int(chat_id)
        msg_id_i = int(msg_id)
        edit_date = int(m.get("edit_date") or 0)
        if owner_id is None:
            _db_put_message(chat_id_i, msg_id_i, m)
            return
        notify_owners: set[int] = {int(owner_id)}
        prev = _db_get_message(chat_id_i, msg_id_i)
        if prev is None:
            old_text = "[старое сообщение не сохранено — бот был подключён позже]"
        else:
            old_text = _msg_text(prev)
        new_text = _msg_text(m)
        try:
            logger.info(
                "EDITED_BUSINESS_MESSAGE routing: cid=%s owner_id=%s editor_id=%s chat_id=%s msg_id=%s changed=%s",
                _extract_business_connection_id(m),
                owner_id,
                editor.get("id"),
                chat_id_i,
                msg_id_i,
                old_text != new_text,
            )
        except Exception:
            pass
        if old_text != new_text and notify_owners:
            editor_id = None
            if editor.get("id") is not None:
                try:
                    editor_id = int(editor.get("id"))
                except Exception:
                    editor_id = None

            if isinstance(editor, dict) and editor.get("id"):
                if int(editor.get("id")) != int(ADMIN_ID) and _db_is_blocked(int(editor.get("id"))):
                    return
                _db_log_event(user_id=int(editor.get("id")), action="edited_message", chat_id=chat_id_i, message_id=msg_id_i)

            now = time.time()
            for oid in sorted(notify_owners):
                # don't notify the editor about their own edit
                if editor_id is not None and int(oid) == int(editor_id):
                    continue
                # Dedup across two business streams by recipient + message_id + edit_date.
                k = ("edit", int(oid), int(msg_id_i), int(edit_date))
                ts = _RECENT_NOTIFY.get(k)
                if ts is not None and (now - ts) < _RECENT_NOTIFY_TTL_SEC:
                    continue
                _RECENT_NOTIFY[k] = now
                try:
                    await _notify_edit(
                        owner_id=int(oid),
                        user=editor,
                        chat_label=_chat_label(m),
                        old_text=old_text,
                        new_text=new_text,
                    )
                except Exception:
                    logger.exception(
                        "Failed to notify edit: cid=%s owner_id=%s chat_id=%s msg_id=%s",
                        _extract_business_connection_id(m),
                        int(oid),
                        chat_id_i,
                        msg_id_i,
                    )
        _db_put_message(chat_id_i, msg_id_i, m)

    # Business deletes
    if "deleted_business_messages" in update:
        d = update["deleted_business_messages"]
        owner_id = _target_owner_for_business(d)
        chat_id = (d.get("chat") or {}).get("id")
        message_ids = d.get("message_ids") or []
        if chat_id is None or not message_ids:
            return

        chat_id_i = int(chat_id)
        if owner_id is None:
            return
        notify_owners: set[int] = {int(owner_id)}

        # If we can't resolve recipients for this chat, don't send anything (privacy-safe)
        if not notify_owners:
            return

        connected_chats.add(chat_id_i)

        now = time.time()
        for mid in message_ids:
            try:
                msg_id_i = int(mid)
            except Exception:
                continue
            prev = _db_get_message(chat_id_i, msg_id_i)
            if prev is None:
                old_text = "[сообщение не сохранено]"
                user = {}
                media_kind = None
                media_path = None
            else:
                old_text = _msg_text(prev)
                user = (prev.get("from") or {}) if isinstance(prev, dict) else {}
                media = _db_get_media(chat_id_i, msg_id_i) or {}
                media_kind = media.get("kind")
                media_path = media.get("path")

            sender_id = None
            if isinstance(user, dict) and user.get("id") is not None:
                try:
                    sender_id = int(user.get("id"))
                except Exception:
                    sender_id = None
                if int(user.get("id")) != int(ADMIN_ID) and _db_is_blocked(int(user.get("id"))):
                    continue
                _db_log_event(user_id=int(user.get("id")), action="deleted_message", chat_id=chat_id_i, message_id=msg_id_i)

            # remove from DB cache now
            _db_delete_message(chat_id_i, msg_id_i)
            _db_delete_media(chat_id_i, msg_id_i)

            for oid in sorted(notify_owners):
                if sender_id is not None and int(oid) == int(sender_id):
                    continue
                # Dedup across two business streams by recipient + message_id.
                k = ("del", int(oid), int(msg_id_i))
                ts = _RECENT_NOTIFY.get(k)
                if ts is not None and (now - ts) < _RECENT_NOTIFY_TTL_SEC:
                    continue
                _RECENT_NOTIFY[k] = now

                try:
                    if media_kind and media_path:
                        await _notify_deleted_media(
                            owner_id=int(oid),
                            user=user if isinstance(user, dict) else {},
                            chat_label=_chat_label(prev if prev is not None else d),
                            text_value=old_text,
                            media_kind=str(media_kind),
                            media_path=str(media_path),
                        )
                    else:
                        await _notify_deleted(
                            owner_id=int(oid),
                            user=user if isinstance(user, dict) else {},
                            chat_label=_chat_label(prev if prev is not None else d),
                            text_value=old_text,
                        )
                except Exception:
                    logger.exception(
                        "Failed to notify delete: cid=%s owner_id=%s chat_id=%s msg_id=%s",
                        _extract_business_connection_id(d),
                        int(oid),
                        chat_id_i,
                        msg_id_i,
                    )

    # Fallback: regular edited_message
    if "edited_message" in update:
        m = update["edited_message"]
        chat_id = m.get("chat", {}).get("id")
        msg_id = m.get("message_id")
        if chat_id is None or msg_id is None:
            return
        chat_id_i = int(chat_id)
        msg_id_i = int(msg_id)
        chat = m.get("chat") or {}
        # Privacy-safe: do not forward edits from non-private chats to any global admin/owner.
        # Only notify in the user's own private chat with the bot.
        if chat.get("type") != "private":
            _db_put_message(chat_id_i, msg_id_i, m)
            return
        prev = _db_get_message(chat_id_i, msg_id_i)
        if prev is None:
            old_text = "[старое сообщение не сохранено — бот был подключён позже]"
        else:
            old_text = _msg_text(prev)
        new_text = _msg_text(m)
        target_owner = chat_id_i
        if old_text != new_text and _db_is_bot_user(int(target_owner)):
            user = m.get("from", {})
            await _notify_edit(
                owner_id=target_owner,
                user=user,
                chat_label=_chat_label(m),
                old_text=old_text,
                new_text=new_text,
            )
        _db_put_message(chat_id_i, msg_id_i, m)


async def main() -> None:
    try:
        # Direct long-polling via Bot API to avoid aiogram "not handled" for new Business update types
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90))
        try:
            logger.info("Bot starting (polling). DB=%s", str(DB_PATH))

            # Ensure webhook mode is disabled; otherwise polling may not receive updates.
            try:
                wh_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
                async with session.post(wh_url, json={"drop_pending_updates": True}) as resp:
                    wh_data = await resp.json()
                if not wh_data.get("ok"):
                    logger.warning("deleteWebhook not ok: %s", wh_data)
                else:
                    logger.info("Webhook disabled")
            except Exception:
                logger.exception("Failed to deleteWebhook")

            offset: Optional[int] = None
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            while True:
                payload: dict[str, Any] = {
                    "timeout": 50,
                    "allowed_updates": [
                        "message",
                        "callback_query",
                        "pre_checkout_query",
                        "business_connection",
                        "business_message",
                        "edited_business_message",
                        "deleted_business_messages",
                    ],
                }
                if offset is not None:
                    payload["offset"] = offset
                async with session.post(url, json=payload) as resp:
                    data = await resp.json()
                if not data.get("ok"):
                    logger.warning("getUpdates not ok: %s", data)
                    await asyncio.sleep(1)
                    continue
                updates = data.get("result", [])
                for upd in updates:
                    try:
                        logger.info("RAW KEYS %s", list(upd.keys()))
                        await _process_update(upd)
                    except Exception:
                        logger.exception("Failed to process update")
                    offset = int(upd.get("update_id", 0)) + 1
        finally:
            await session.close()
    finally:
        await bot.session.close()


if __name__ == "__main__":
    # Optional keep-alive HTTP endpoint (useful for Replit).
    # Enable with: KEEPALIVE_HTTP=1
    if os.getenv("KEEPALIVE_HTTP") == "1":
        from flask import Flask
        import threading

        app = Flask(__name__)

        @app.route("/")
        def home():
            return "Bot is alive"

        def run_flask():
            app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

        threading.Thread(target=run_flask, daemon=True).start()

    asyncio.run(main())
