"""
install_state.py — Sprint Install Indicator B (06/05).

Shared state pro progresso de first-run setup. Atualizado por setup
modules (vendor_downloader, setup_editor_download, setup_cs2_icons),
lido pelo install_progress_server.py que expõe via HTTP.

Mathieu spec: site detecta quando .exe foi clicado, em vez de quando
download começou. Beacon-style: client reporta "estou rodando + setup
em progresso" via localhost:5775. Web polla.

Phases (ordem cronológica):
  starting           → main.py boot inicial, antes de setup
  downloading_hlae   → vendor_downloader baixando HLAE+ffmpeg
  downloading_node   → vendor_downloader baixando Node 20
  downloading_editor → vendor_downloader baixando vendor-editor.zip
  extracting_icons   → setup_cs2_icons.py rodando
  ready              → setup completo, full Flask pode subir

Estado threadsafe — lock-based pra leitura concorrente do HTTP server +
escrita do setup thread.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Optional


_lock = threading.Lock()
_state: dict[str, Any] = {
    "phase": "starting",
    "phase_label": "Iniciando…",
    "components": {},  # name → {downloaded, total, pct, label}
    "started_at": time.time(),
    "ready": False,
}


# Phase labels for UI consumption — Portuguese friendly.
PHASE_LABELS = {
    "starting": "Iniciando…",
    "downloading_hlae": "Baixando HLAE + ffmpeg…",
    "downloading_node": "Baixando Node 20…",
    "downloading_editor": "Baixando editor Remotion…",
    "extracting_icons": "Extraindo ícones do CS2…",
    "ready": "Pronto",
}


def update_phase(phase: str, label: Optional[str] = None) -> None:
    """Set current setup phase. label opcional — fallback pra PHASE_LABELS."""
    with _lock:
        _state["phase"] = phase
        _state["phase_label"] = label or PHASE_LABELS.get(phase, phase)
        if phase == "ready":
            _state["ready"] = True


def update_component(name: str, downloaded: int, total: int) -> None:
    """Update progresso de download de UM componente. Idempotente.

    Args:
      name: identifier ("HLAE", "Node 20", "Editor Remotion", etc)
      downloaded: bytes baixados até agora
      total: bytes totais (ou 0 se desconhecido)
    """
    with _lock:
        pct = int(downloaded / total * 100) if total > 0 else 0
        _state["components"][name] = {
            "downloaded": downloaded,
            "total": total,
            "pct": pct,
            "label": name,
        }


def mark_ready() -> None:
    """Sinaliza que setup completou. Web banner some quando vê isso."""
    update_phase("ready")


def get_state() -> dict[str, Any]:
    """Snapshot atual do estado pra HTTP response. Threadsafe."""
    with _lock:
        return {
            "phase": _state["phase"],
            "phase_label": _state["phase_label"],
            "components": dict(_state["components"]),
            "started_at": _state["started_at"],
            "elapsed_sec": time.time() - _state["started_at"],
            "ready": _state["ready"],
        }
