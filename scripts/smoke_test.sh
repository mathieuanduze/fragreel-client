#!/usr/bin/env bash
# FragReel smoke test — valida deploy v0.2.11 (web + GitHub release + client local)
#
# Uso:
#   ./scripts/smoke_test.sh                # roda tudo
#   ./scripts/smoke_test.sh --web-only     # só Vercel
#   ./scripts/smoke_test.sh --github-only  # só GitHub release
#   ./scripts/smoke_test.sh --local-only   # só client local (precisa estar rodando)
#   EXPECTED_VERSION=0.2.12 ./scripts/smoke_test.sh   # checa outra versão
#
# Códigos de saída:
#   0 = tudo passou
#   1 = pelo menos um check falhou
#   2 = erro de uso (flag inválida, deps faltando, etc)

set -uo pipefail

# ───────── config ─────────
EXPECTED_VERSION="${EXPECTED_VERSION:-0.2.11}"
WEB_URL="${WEB_URL:-https://fragreel.vercel.app}"
LOCAL_URL="${LOCAL_URL:-http://127.0.0.1:5775}"
GITHUB_REPO="${GITHUB_REPO:-mathieuanduze/fragreel-client}"
MIN_EXE_SIZE_MB=200    # release válido tem ~250MB; <200 vira red flag

# ───────── cores ─────────
if [[ -t 1 ]]; then
  C_GREEN=$'\033[0;32m'; C_RED=$'\033[0;31m'; C_YELLOW=$'\033[0;33m'
  C_BLUE=$'\033[0;34m';  C_DIM=$'\033[2m';    C_RESET=$'\033[0m'
else
  C_GREEN=""; C_RED=""; C_YELLOW=""; C_BLUE=""; C_DIM=""; C_RESET=""
fi

PASS=0; FAIL=0; SKIP=0

ok()    { echo "  ${C_GREEN}✓${C_RESET} $*"; PASS=$((PASS+1)); }
fail()  { echo "  ${C_RED}✗${C_RESET} $*"; FAIL=$((FAIL+1)); }
warn()  { echo "  ${C_YELLOW}⚠${C_RESET} $*"; }
skip()  { echo "  ${C_DIM}○ $*${C_RESET}"; SKIP=$((SKIP+1)); }
section() { echo; echo "${C_BLUE}━━━ $* ━━━${C_RESET}"; }

# ───────── deps ─────────
need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Faltando: $1"; exit 2; }
}
need curl

# Python pra parse de JSON — tenta python3, python, py nessa ordem
# (Windows/Git Bash às vezes só tem `python` ou `py`).
if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
elif command -v py >/dev/null 2>&1; then
  PY="py -3"
else
  echo "Faltando: python3/python/py"; exit 2
fi

# ───────── flags ─────────
RUN_WEB=true; RUN_GITHUB=true; RUN_LOCAL=true
case "${1:-}" in
  --web-only)    RUN_GITHUB=false; RUN_LOCAL=false ;;
  --github-only) RUN_WEB=false;    RUN_LOCAL=false ;;
  --local-only)  RUN_WEB=false;    RUN_GITHUB=false ;;
  ""|--all) ;;
  -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
  *) echo "Flag inválida: $1"; exit 2 ;;
esac

echo "${C_BLUE}FragReel smoke test${C_RESET} — esperando ${C_YELLOW}v${EXPECTED_VERSION}${C_RESET}"

# ════════════════════════════════════════════════════════════════
# 1. WEB (Vercel)
# ════════════════════════════════════════════════════════════════
if $RUN_WEB; then
  section "Web (Vercel: $WEB_URL)"

  # 1a. Status code
  status=$(curl -sS -o /dev/null -w "%{http_code}" --max-time 10 "$WEB_URL" || echo "000")
  if [[ "$status" == "200" ]]; then
    ok "GET / → HTTP 200"
  else
    fail "GET / → HTTP $status (esperado 200)"
  fi

  # 1b. Versão exposta no bundle JS
  # CLIENT_VERSION="v0.2.11" vai parar minificado nos chunks do Next.js.
  # Buscamos a string em todos os chunks _next/static/chunks linkados na home.
  html=$(curl -sS --max-time 10 "$WEB_URL" || echo "")
  if [[ -z "$html" ]]; then
    fail "HTML da home veio vazio — não dá pra checar versão"
  else
    # extrai paths de chunks JS do HTML
    chunks=$(echo "$html" | grep -oE '/_next/static/chunks/[^"'\'']+\.js' | sort -u | head -20)
    if [[ -z "$chunks" ]]; then
      warn "Nenhum chunk Next.js encontrado no HTML — site usa estrutura diferente?"
    else
      found=false
      for chunk in $chunks; do
        if curl -sS --max-time 8 "${WEB_URL}${chunk}" 2>/dev/null | grep -q "v${EXPECTED_VERSION}"; then
          ok "Bundle JS anuncia v${EXPECTED_VERSION} (em ${chunk##*/})"
          found=true
          break
        fi
      done
      $found || fail "v${EXPECTED_VERSION} não encontrada em nenhum dos $(echo "$chunks" | wc -l | tr -d ' ') chunks JS"
    fi
  fi

  # 1c. /download → redireciona pro release no GitHub
  download_status=$(curl -sS -o /dev/null -w "%{http_code}" --max-time 10 "$WEB_URL/download" || echo "000")
  if [[ "$download_status" == "302" || "$download_status" == "301" || "$download_status" == "307" || "$download_status" == "308" ]]; then
    location=$(curl -sSI --max-time 10 "$WEB_URL/download" 2>/dev/null | grep -i '^location:' | tr -d '\r' | awk '{print $2}')
    if [[ "$location" == *"github.com"*"FragReel.exe"* ]]; then
      ok "/download redireciona pro GitHub release ($download_status)"
    else
      warn "/download retorna $download_status mas Location inesperado: ${location:-<vazio>}"
    fi
  elif [[ "$download_status" == "200" ]]; then
    warn "/download retorna 200 (esperava redirect — pode ser proxy direto, validar manual)"
  else
    fail "/download → HTTP $download_status (esperava 30x)"
  fi
fi

# ════════════════════════════════════════════════════════════════
# 2. GITHUB RELEASE
# ════════════════════════════════════════════════════════════════
if $RUN_GITHUB; then
  section "GitHub Release ($GITHUB_REPO)"

  release_json=$(curl -sS --max-time 10 \
    "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" 2>/dev/null || echo "{}")

  tag=$(echo "$release_json" | $PY -c "import sys,json; print(json.load(sys.stdin).get('tag_name',''))" 2>/dev/null)
  if [[ -z "$tag" ]]; then
    fail "API do GitHub não retornou tag — repo privado? sem release? rate-limited?"
  elif [[ "$tag" == "v${EXPECTED_VERSION}" ]]; then
    ok "Tag latest = ${tag}"
  else
    fail "Tag latest = ${tag} (esperado v${EXPECTED_VERSION})"
  fi

  # asset FragReel.exe
  exe_size=$(echo "$release_json" | $PY -c "
import sys, json
data = json.load(sys.stdin)
for a in data.get('assets', []):
    if a.get('name') == 'FragReel.exe':
        print(a.get('size', 0)); break
else:
    print(0)
" 2>/dev/null)

  if [[ "$exe_size" -gt $((MIN_EXE_SIZE_MB * 1024 * 1024)) ]]; then
    size_mb=$((exe_size / 1024 / 1024))
    ok "FragReel.exe publicado (${size_mb} MB)"
  elif [[ "$exe_size" -gt 0 ]]; then
    size_mb=$((exe_size / 1024 / 1024))
    fail "FragReel.exe muito pequeno (${size_mb} MB, mínimo ${MIN_EXE_SIZE_MB} MB) — build incompleto?"
  else
    fail "FragReel.exe não encontrado nos assets do release"
  fi

  # download URL realmente serve (HEAD pra não baixar 250MB)
  download_url="https://github.com/${GITHUB_REPO}/releases/latest/download/FragReel.exe"
  final_status=$(curl -sLI --max-time 15 -o /dev/null -w "%{http_code}" "$download_url" 2>/dev/null || echo "000")
  if [[ "$final_status" == "200" ]]; then
    ok "Download URL serve o .exe (HTTP 200 após redirects)"
  else
    fail "Download URL retornou HTTP $final_status"
  fi
fi

# ════════════════════════════════════════════════════════════════
# 3. CLIENT LOCAL (só se estiver rodando)
# ════════════════════════════════════════════════════════════════
if $RUN_LOCAL; then
  section "Client local ($LOCAL_URL)"

  # 3a. Health probe — se não respondeu, pula em vez de falhar
  if ! curl -sS --max-time 2 "$LOCAL_URL/health" >/dev/null 2>&1; then
    skip "Client não está rodando em $LOCAL_URL — pulando checks locais"
    skip "(use --local-only com o client aberto pra testar essa parte)"
  else
    ok "GET /health respondeu"

    # 3b. Versão
    version_json=$(curl -sS --max-time 5 "$LOCAL_URL/version" 2>/dev/null || echo "{}")
    local_version=$(echo "$version_json" | $PY -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('version') or data.get('client_version') or '')
except Exception:
    print('')
" 2>/dev/null)

    if [[ "$local_version" == "$EXPECTED_VERSION" || "$local_version" == "v$EXPECTED_VERSION" ]]; then
      ok "Client local = v${local_version#v}"
    elif [[ -n "$local_version" ]]; then
      fail "Client local = v${local_version#v} (esperado v${EXPECTED_VERSION}) — atualizar"
    else
      fail "/version retornou JSON sem campo version: $version_json"
    fi

    # 3c. Endpoint /update existe (POST com body vazio só pra ver se rota tá registrada)
    # Aceita 200, 400, 409, 501 — qualquer coisa que NÃO seja 404
    update_status=$(curl -sS -o /dev/null -w "%{http_code}" --max-time 5 \
      -X POST "$LOCAL_URL/update" -H "Content-Type: application/json" -d '{}' 2>/dev/null || echo "000")
    if [[ "$update_status" == "404" ]]; then
      fail "POST /update → 404 (endpoint de auto-update faltando — client pré-v0.2.11?)"
    elif [[ "$update_status" == "000" ]]; then
      fail "POST /update → sem resposta (timeout/erro de conexão)"
    else
      ok "POST /update existe (retornou $update_status — esperado já que estamos na versão atual)"
    fi
  fi
fi

# ════════════════════════════════════════════════════════════════
# resumo
# ════════════════════════════════════════════════════════════════
echo
echo "${C_BLUE}━━━ resumo ━━━${C_RESET}"
echo "  ${C_GREEN}passou:${C_RESET}  $PASS"
echo "  ${C_RED}falhou:${C_RESET}  $FAIL"
echo "  ${C_DIM}skipped: $SKIP${C_RESET}"

if [[ "$FAIL" -gt 0 ]]; then
  echo
  echo "${C_RED}✗ Smoke FALHOU — investigar antes de promover.${C_RESET}"
  exit 1
fi

echo
echo "${C_GREEN}✓ Smoke OK — v${EXPECTED_VERSION} validada.${C_RESET}"
exit 0
