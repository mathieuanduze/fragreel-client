#!/usr/bin/env node
/**
 * steam_gc.js — Steam GC sidecar bot pro FragReel.
 *
 * Sprint DEMO-3 (08/05/2026) — auto match history sem abrir CS2.
 *
 * Arquitetura:
 *   FragReel.exe (Python parent, PyInstaller)
 *     └── spawn child: node steam_gc.js
 *           └── IPC stdin/stdout JSON-line protocol
 *                 ↑↓ Python wrapper steam_gc.py envia/recebe requests
 *
 * Por que Node em vez de Python ValvePython:
 *   ValvePython libs (csgo + steam) abandonadas pra CS2 (last commit fev/2021).
 *   GitHub issues #62, #63 confirmam GC connection broken pós CS2 update.
 *   Node DoctorMcKay/steam-user (dez/2025) + globaloffensive (mar/2026)
 *   ativamente mantidos com suporte CS2. Reference impl: cs-demo-manager.
 *
 * IPC protocol:
 *   Request:  {"id": "uuid", "action": "ping" | "login" | "match_history" | "resolve_sharecode" | "shutdown", "params": {...}}
 *   Response: {"id": "uuid", "ok": true|false, "data": {...} | "error": "..."}
 *
 * Lifecycle:
 *   - Parent spawn → sidecar emite {"event": "ready"} no stdout
 *   - Parent envia requests via stdin (JSON line per request)
 *   - Parent envia {"action": "shutdown"} → sidecar disconnect graceful + exit 0
 *   - Se parent morrer (SIGTERM/disconnect stdin), sidecar detecta + auto-exit
 */

const SteamUser = require("steam-user");
const GlobalOffensive = require("globaloffensive");
const readline = require("readline");

// ── Singleton state ─────────────────────────────────────────────────────────
let steam = null;       // SteamUser instance (login + auth)
let csgo = null;        // GlobalOffensive instance (GC connection)
let loginState = "logged-out"; // "logged-out" | "logging-in" | "logged-in" | "error"
let gcConnected = false;
let lastError = null;

// ── IPC helpers ─────────────────────────────────────────────────────────────
function sendResponse(id, ok, payload) {
  const msg = ok ? { id, ok: true, data: payload } : { id, ok: false, error: payload };
  process.stdout.write(JSON.stringify(msg) + "\n");
}

function sendEvent(event, data = {}) {
  process.stdout.write(JSON.stringify({ event, ...data }) + "\n");
}

function logStderr(...args) {
  // stderr é debug-only, não interfere com IPC stdout
  process.stderr.write(`[steam_gc] ${args.join(" ")}\n`);
}

// ── Action handlers ─────────────────────────────────────────────────────────

/** ping: sanity check sidecar tá vivo */
function handlePing(id, params) {
  sendResponse(id, true, {
    pong: true,
    timestamp: Date.now(),
    version: "0.1.0",
    login_state: loginState,
    gc_connected: gcConnected,
  });
}

/** status: snapshot do estado atual sem side-effects */
function handleStatus(id, params) {
  sendResponse(id, true, {
    login_state: loginState,
    gc_connected: gcConnected,
    last_error: lastError,
    steamid: steam && steam.steamID ? steam.steamID.getSteamID64() : null,
  });
}

/**
 * login: faz login Steam com credentials + Steam Guard code.
 *
 * params: {
 *   refresh_token?: string  // refresh token salvo (se existir, prioritário)
 *   account_name?: string   // username
 *   password?: string       // senha
 *   auth_code?: string      // Steam Guard code (se exigido)
 *   two_factor_code?: string // Steam Mobile Authenticator code
 * }
 *
 * Resposta sucesso: {steamid64, refresh_token (pra cache)}
 * Resposta erro: error string
 *
 * Sprint 1 MVP: stub. Sprint 2 implementa real login + token persistence.
 */
function handleLogin(id, params) {
  if (!params || (!params.refresh_token && !params.account_name)) {
    sendResponse(id, false, "missing credentials (refresh_token OR account_name+password)");
    return;
  }
  if (loginState === "logging-in") {
    sendResponse(id, false, "login already in progress");
    return;
  }
  if (loginState === "logged-in") {
    sendResponse(id, true, {
      already_logged_in: true,
      steamid64: steam.steamID.getSteamID64(),
    });
    return;
  }

  loginState = "logging-in";
  lastError = null;
  steam = new SteamUser();
  csgo = new GlobalOffensive(steam);

  // Event handlers
  steam.on("loggedOn", () => {
    loginState = "logged-in";
    logStderr("logged in as", steam.steamID.getSteamID64());
    // Solicita CS2 game state pra GC connection (CS2 app id 730)
    steam.gamesPlayed([730]);
  });

  steam.on("error", (err) => {
    loginState = "error";
    lastError = String(err);
    logStderr("steam error:", err);
    sendResponse(id, false, lastError);
  });

  steam.on("steamGuard", (domain, callback) => {
    // Steam Guard code requerido — sinalizar parent
    sendEvent("steam_guard_required", { domain });
    if (params.auth_code) {
      callback(params.auth_code);
    } else {
      sendResponse(id, false, "steam_guard_required");
    }
  });

  csgo.on("connectedToGC", () => {
    gcConnected = true;
    logStderr("GC connected");
    sendResponse(id, true, {
      steamid64: steam.steamID.getSteamID64(),
      refresh_token: steam.logOnDetails && steam.logOnDetails.refresh_token,
    });
  });

  csgo.on("disconnectedFromGC", (reason) => {
    gcConnected = false;
    logStderr("GC disconnected:", reason);
  });

  // Trigger login
  try {
    if (params.refresh_token) {
      steam.logOn({ refreshToken: params.refresh_token });
    } else {
      const logonOpts = {
        accountName: params.account_name,
        password: params.password,
      };
      if (params.two_factor_code) logonOpts.twoFactorCode = params.two_factor_code;
      if (params.auth_code) logonOpts.authCode = params.auth_code;
      steam.logOn(logonOpts);
    }
  } catch (e) {
    loginState = "error";
    lastError = String(e);
    sendResponse(id, false, lastError);
  }
}

/**
 * match_history: puxa lista de sharecodes do user via GC.
 *
 * params: {
 *   steamid64: string,
 *   match_sharing_auth_code: string,  // 4-char code da página Steam
 *   known_sharecode?: string  // último sharecode conhecido (pra cursor)
 * }
 *
 * Sprint 2 implementa. Sprint 1 stub.
 */
function handleMatchHistory(id, params) {
  if (loginState !== "logged-in" || !gcConnected) {
    sendResponse(id, false, "not_logged_in_to_gc");
    return;
  }
  if (!params || !params.steamid64 || !params.match_sharing_auth_code) {
    sendResponse(id, false, "missing params (steamid64 + match_sharing_auth_code)");
    return;
  }

  // Stub Sprint 1 — implementação real Sprint 2
  sendResponse(id, false, "not_implemented_sprint_2");

  // Sprint 2 será algo tipo:
  //   csgo.requestPlayersProfile(steamid64, params.match_sharing_auth_code, ...)
  //   ou csgo.requestRecentMatches(steamid64, ...)
  //   Verificar API exata via lib docs
}

/**
 * resolve_sharecode: recebe sharecode "CSGO-XXX-XXX-..." → match metadata + demo URL.
 *
 * Sprint 2 implementa. Sprint 1 stub.
 */
function handleResolveSharecode(id, params) {
  if (loginState !== "logged-in" || !gcConnected) {
    sendResponse(id, false, "not_logged_in_to_gc");
    return;
  }
  if (!params || !params.sharecode) {
    sendResponse(id, false, "missing sharecode");
    return;
  }

  // Stub Sprint 1 — implementação real Sprint 2
  sendResponse(id, false, "not_implemented_sprint_2");
}

/** shutdown: graceful exit */
function handleShutdown(id, params) {
  logStderr("shutdown requested");
  sendResponse(id, true, { goodbye: true });
  setTimeout(() => {
    if (steam) {
      try {
        steam.logOff();
      } catch (e) {
        // ignore
      }
    }
    process.exit(0);
  }, 100);
}

// ── Action dispatcher ───────────────────────────────────────────────────────
const HANDLERS = {
  ping: handlePing,
  status: handleStatus,
  login: handleLogin,
  match_history: handleMatchHistory,
  resolve_sharecode: handleResolveSharecode,
  shutdown: handleShutdown,
};

function dispatch(req) {
  const { id, action, params } = req;
  if (!id || !action) {
    sendResponse(id || "unknown", false, "missing id or action");
    return;
  }
  const handler = HANDLERS[action];
  if (!handler) {
    sendResponse(id, false, `unknown action: ${action}`);
    return;
  }
  try {
    handler(id, params || {});
  } catch (e) {
    logStderr("handler error:", e);
    sendResponse(id, false, `handler_error: ${String(e)}`);
  }
}

// ── stdin reader (line-based JSON) ──────────────────────────────────────────
const rl = readline.createInterface({ input: process.stdin });

rl.on("line", (line) => {
  line = line.trim();
  if (!line) return;
  let req;
  try {
    req = JSON.parse(line);
  } catch (e) {
    logStderr("invalid JSON:", line);
    return;
  }
  dispatch(req);
});

rl.on("close", () => {
  // Parent fechou stdin — auto-shutdown
  logStderr("parent stdin closed, shutting down");
  if (steam) {
    try {
      steam.logOff();
    } catch (e) {
      // ignore
    }
  }
  process.exit(0);
});

// Graceful SIGTERM (Python parent termina via subprocess.terminate())
process.on("SIGTERM", () => {
  logStderr("SIGTERM received");
  if (steam) {
    try {
      steam.logOff();
    } catch (e) {
      // ignore
    }
  }
  process.exit(0);
});

// Anuncia ready
sendEvent("ready", { version: "0.1.0", node: process.version });
logStderr("steam_gc.js ready, version 0.1.0, node", process.version);
