"""Single source of truth pra versão do client.

⚠️ NÃO editar __version__ manualmente — é regenerado a partir da tag git.

Fluxos:

1. **Build de release (CI, tag push)**
   `.github/workflows/release.yml` sobrescreve este arquivo inteiro com
   `__version__ = "<tag>"` ANTES de rodar o PyInstaller. O .exe final
   sempre tem a versão da tag que disparou o build.

2. **Build manual (workflow_dispatch)**
   CI sobrescreve com `__version__ = "v0.0.0-manual-<sha>"` pra deixar
   explícito que não é release oficial.

3. **Rodar do source (dev, fora do CI)**
   O fallback abaixo usa `git describe --tags --always --dirty` pra
   gerar uma string tipo `v0.2.15-3-gabc1234-dirty`. Se o `git` falhar
   (repo sem tags, sem git instalado, .exe descongelado em pasta sem
   .git), cai pra `v0.0.0-dev`.

O endpoint /version expõe `__version__` pra que a web detecte updates
disponíveis — logo, se aparecer `v0.0.0-dev` em produção é sinal de que
o CI não rodou o step `Inject version from tag`.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _dev_version() -> str:
    # PyInstaller-frozen .exe nunca cai neste path — o CI reescreve este
    # arquivo inteiro antes do build. Mas por via das dúvidas, se um .exe
    # freezado chegar aqui (dev buildou local e copiou pra outra máquina),
    # não tenta rodar git: não tem .git ao lado do .exe.
    if getattr(sys, "frozen", False):
        return "v0.0.0-dev"

    try:
        repo = Path(__file__).resolve().parent
        out = subprocess.check_output(
            ["git", "-C", str(repo), "describe", "--tags", "--always", "--dirty"],
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        return out.decode().strip() or "v0.0.0-dev"
    except Exception:
        return "v0.0.0-dev"


__version__ = _dev_version()
