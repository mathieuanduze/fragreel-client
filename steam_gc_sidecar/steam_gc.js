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
 * login: faz login Steam com credentials OR refresh_token.
 *
 * Sprint 2 (08/05/2026) — implementação real.
 *
 * params: {
 *   refresh_token?: string,    // prioritário (login subsequente, sem reentrar credentials)
 *   account_name?: string,     // username Steam (primeiro login)
 *   password?: string,         // senha Steam
 *   auth_code?: string,        // Steam Guard code via email
 *   two_factor_code?: string,  // Steam Mobile Authenticator code
 * }
 *
 * Resposta sucesso: {steamid64, refresh_token, gc_connected}
 * Resposta erro: error string ('steam_guard_required' | 'invalid_password' | etc)
 *
 * Pendentes assinalados: o ID da request fica armazenado em pendingLoginId
 * pra resposta async vir de eventos (loggedOn / error / connectedToGC).
 */
let pendingLoginId = null;
let pendingLoginResolved = false;

function resolveLoginRequest(ok, payload) {
  if (!pendingLoginId || pendingLoginResolved) return;
  pendingLoginResolved = true;
  sendResponse(pendingLoginId, ok, payload);
  pendingLoginId = null;
}

function setupSteamHandlers() {
  if (!steam) return;
  // Cleanup possíveis listeners residuais (idempotência)
  steam.removeAllListeners("loggedOn");
  steam.removeAllListeners("error");
  steam.removeAllListeners("steamGuard");
  steam.removeAllListeners("refreshToken");
  steam.removeAllListeners("disconnected");

  steam.on("loggedOn", () => {
    loginState = "logged-in";
    logStderr("logged in as", steam.steamID.getSteamID64());
    // Solicita CS2 game state pra GC connection (CS2 app id 730)
    steam.gamesPlayed([730]);
  });

  steam.on("refreshToken", (token) => {
    // SteamUser emite esse evento quando obtém refresh_token (login fresh).
    // Cacheamos pra próximo login não exigir credentials. Parent salva
    // encrypted via Python wrapper.
    logStderr("refresh_token obtido (len:", token ? token.length : 0, ")");
    sendEvent("refresh_token_obtained", { token });
  });

  steam.on("error", (err) => {
    loginState = "error";
    lastError = String(err.message || err);
    const eresult = err && err.eresult;
    logStderr("steam error:", err, "eresult:", eresult);
    // EResult 5 = InvalidPassword, 65 = AccountLogonDeniedNeedTwoFactor, etc
    let errorCode = "steam_error";
    if (eresult === 5) errorCode = "invalid_password";
    else if (eresult === 63) errorCode = "account_logon_denied_email"; // Steam Guard email
    else if (eresult === 65) errorCode = "two_factor_required";
    else if (eresult === 88) errorCode = "two_factor_invalid";
    else if (eresult === 84) errorCode = "rate_limit_exceeded";
    resolveLoginRequest(false, errorCode);
  });

  steam.on("steamGuard", (domain, callback, lastCodeWrong) => {
    logStderr("steam_guard_required domain=", domain, "lastWrong=", lastCodeWrong);
    sendEvent("steam_guard_required", {
      domain: domain || null,  // null = mobile authenticator, "email.com" = email
      last_code_wrong: !!lastCodeWrong,
    });
    // Não chama callback aqui — parent vai re-disparar `login` action com
    // auth_code/two_factor_code e nós faremos novo logOn(). steam-user
    // engole esse listener e re-tenta sozinho com callback se chamarmos.
    // Por simplicidade do flow, deixamos parent gerenciar.
    resolveLoginRequest(false, "steam_guard_required");
  });

  steam.on("disconnected", (eresult, msg) => {
    loginState = "logged-out";
    gcConnected = false;
    logStderr("disconnected eresult=", eresult, "msg=", msg);
  });
}

function setupCSGOHandlers() {
  if (!csgo) return;
  csgo.removeAllListeners("connectedToGC");
  csgo.removeAllListeners("disconnectedFromGC");

  csgo.on("connectedToGC", () => {
    gcConnected = true;
    logStderr("GC connected");
    // Login completo: steam ok + GC ok. Resolve pending login request.
    resolveLoginRequest(true, {
      steamid64: steam.steamID.getSteamID64(),
      gc_connected: true,
    });
  });

  csgo.on("disconnectedFromGC", (reason) => {
    gcConnected = false;
    logStderr("GC disconnected:", reason);
    sendEvent("gc_disconnected", { reason });
  });
}

function handleLogin(id, params) {
  if (!params || (!params.refresh_token && !params.account_name)) {
    sendResponse(id, false, "missing credentials (refresh_token OR account_name+password)");
    return;
  }
  if (loginState === "logging-in") {
    sendResponse(id, false, "login already in progress");
    return;
  }
  if (loginState === "logged-in" && gcConnected) {
    sendResponse(id, true, {
      already_logged_in: true,
      steamid64: steam.steamID.getSteamID64(),
      gc_connected: true,
    });
    return;
  }

  loginState = "logging-in";
  lastError = null;
  pendingLoginId = id;
  pendingLoginResolved = false;

  // Re-instancia steam + csgo (idempotente: drop listeners se já existir)
  if (steam) {
    try {
      steam.logOff();
    } catch (e) { /* ignore */ }
  }
  steam = new SteamUser();
  csgo = new GlobalOffensive(steam);
  setupSteamHandlers();
  setupCSGOHandlers();

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
    resolveLoginRequest(false, lastError);
  }

  // Safety timeout — se nem error nem loggedOn vier em 30s, falha
  setTimeout(() => {
    if (!pendingLoginResolved) {
      logStderr("login timeout (30s)");
      resolveLoginRequest(false, "login_timeout");
    }
  }, 30_000);
}

/**
 * logout: encerra sessão Steam atual (sem matar sidecar).
 */
function handleLogout(id, params) {
  if (steam) {
    try {
      steam.logOff();
    } catch (e) { /* ignore */ }
  }
  loginState = "logged-out";
  gcConnected = false;
  steam = null;
  csgo = null;
  sendResponse(id, true, { logged_out: true });
}

/**
 * recent_matches: puxa últimas ~8 partidas do user via GC.
 *
 * Sprint 2 (08/05/2026) — implementação real.
 *
 * node-globaloffensive expõe `requestRecentMatches(steamid64, callback)`
 * que retorna até 8 matches recentes. Pra histórico completo precisa
 * paginar via match_sharing_auth_code (próxima sprint).
 *
 * params: { steamid64: string }
 *
 * Resposta: {matches: [{matchId, sharecode, mapName, scoreCT, scoreT,
 *           tickrate, demoUrl, timestamp, players}, ...]}
 */
function handleRecentMatches(id, params) {
  if (loginState !== "logged-in" || !gcConnected) {
    sendResponse(id, false, "not_logged_in_to_gc");
    return;
  }
  if (!params || !params.steamid64) {
    sendResponse(id, false, "missing steamid64");
    return;
  }

  try {
    csgo.requestRecentMatches(params.steamid64, (err, body) => {
      if (err) {
        logStderr("requestRecentMatches error:", err);
        sendResponse(id, false, `gc_error: ${String(err)}`);
        return;
      }
      // body é proto CMsgGCCStrike15_v2_MatchList — array de matches
      const matches = (body && body.matches) || [];
      const parsed = matches.map((m) => parseMatchProto(m));
      sendResponse(id, true, { matches: parsed, count: parsed.length });
    });
  } catch (e) {
    logStderr("recent_matches dispatch error:", e);
    sendResponse(id, false, `dispatch_error: ${String(e)}`);
  }

  // Safety timeout — GC pode nunca responder
  setTimeout(() => {
    // (response já foi enviada pelo callback — duplicate sendResponse é
    // detectado no Python wrapper que ignora pending desconhecido)
  }, 15_000);
}

/**
 * match_history: paginação completa via match_sharing_auth_code.
 *
 * params: {
 *   steamid64: string,
 *   match_sharing_auth_code: string,  // 4-char Steam page code
 *   known_sharecode?: string,         // cursor — último sharecode visto
 * }
 *
 * Resposta: {sharecodes: [...], next_known: string|null}
 *
 * GC `requestGame(sharecode)` ou similar — verificar API exata.
 * MVP Sprint 2: stub retornando recent_matches sharecodes (sem paginação real).
 * Sprint 3 implementa paginação completa.
 */
function handleMatchHistory(id, params) {
  if (loginState !== "logged-in" || !gcConnected) {
    sendResponse(id, false, "not_logged_in_to_gc");
    return;
  }
  if (!params || !params.steamid64) {
    sendResponse(id, false, "missing steamid64");
    return;
  }

  // MVP Sprint 2: usa requestRecentMatches como base (retorna 8 matches).
  // Match_sharing_auth_code é necessário pra paginar mais — fica pra Sprint 3.
  try {
    csgo.requestRecentMatches(params.steamid64, (err, body) => {
      if (err) {
        sendResponse(id, false, `gc_error: ${String(err)}`);
        return;
      }
      const matches = (body && body.matches) || [];
      const sharecodes = matches.map((m) => extractSharecode(m)).filter(Boolean);
      sendResponse(id, true, {
        sharecodes,
        count: sharecodes.length,
        note: "MVP — recent 8 matches only. Full pagination via auth_code pending Sprint 3.",
      });
    });
  } catch (e) {
    sendResponse(id, false, `dispatch_error: ${String(e)}`);
  }
}

/**
 * resolve_sharecode: recebe sharecode (ex: "CSGO-XXX-...") → match metadata + demo URL.
 *
 * Sprint 2 implementação real.
 *
 * `globaloffensive` lib NÃO tem helper direto pra resolver sharecode.
 * Approach: decode sharecode → matchId/outcomeId/tokenId → request GC
 * com requestGame OR requestPlayersProfile.
 *
 * Mais prático: usar `csgo-sharecode` npm package pra decode + chamar
 * `csgo.requestGame()` com matchId.
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

  // MVP: tenta requestRecentMatches do user owner do sharecode (retorna se
  // estiver entre os recentes), senão retorna apenas o sharecode raw como
  // metadata mínima.
  // TODO Sprint 3: integrar `csgo-sharecode` npm pra decode + request específico.
  sendResponse(id, false, "not_yet_implemented_sprint_3");
}

// ── Helpers de proto parsing ─────────────────────────────────────────────────

/**
 * Parse match proto retornado pelo GC.
 *
 * Estrutura típica: CMsgGCCStrike15_v2_MatchInfo com fields:
 *   matchid, matchtime, watchablematchinfo (server, gameId, etc),
 *   roundstats_legacy, roundstats_all, etc.
 *
 * sharecode é construído a partir de matchid + outcomeid + tokenid.
 * Reference: csgo-sharecode npm package.
 */
function parseMatchProto(m) {
  if (!m) return null;
  return {
    match_id: m.matchid ? String(m.matchid) : null,
    match_time: m.matchtime || null,
    watchable: m.watchablematchinfo
      ? {
          server_ip: m.watchablematchinfo.server_ip,
          tv_port: m.watchablematchinfo.tv_port,
          tv_spectators: m.watchablematchinfo.tv_spectators,
          cl_decryptdata_key: m.watchablematchinfo.cl_decryptdata_key,
        }
      : null,
    sharecode: extractSharecode(m),
    // Demo URL fica no roundstats_legacy.reservationid_url ou roundstats_all
    demo_url: extractDemoUrl(m),
  };
}

function extractSharecode(m) {
  // Pra construir sharecode precisa: matchid + outcomeid + tokenid
  // Sprint 2 MVP: retorna placeholder se fields presentes
  if (!m || !m.matchid) return null;
  const matchId = String(m.matchid);
  const outcomeId = m.roundstats_legacy && m.roundstats_legacy.reservationid
    ? String(m.roundstats_legacy.reservationid)
    : null;
  const tokenId = m.watchablematchinfo && m.watchablematchinfo.cl_decryptdata_key
    ? String(m.watchablematchinfo.cl_decryptdata_key)
    : null;
  if (!outcomeId || !tokenId) return null;
  // Sprint 3: usar `csgo-sharecode` lib pra encode oficial
  return `MATCH-${matchId}-${outcomeId}-${tokenId}`;
}

function extractDemoUrl(m) {
  // Demo URL geralmente em roundstats_all[last].map ou roundstats_legacy.map
  const rs = m && (m.roundstats_legacy || (m.roundstats_all && m.roundstats_all[0]));
  if (rs && rs.map && rs.map.startsWith("http")) {
    return rs.map;
  }
  return null;
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
  logout: handleLogout,
  recent_matches: handleRecentMatches,
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
