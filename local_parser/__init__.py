"""
local_parser — Sprint I.5 (28/04 noite): cópia do `api/parser/demo_parser.py`
do repo fragreel pro cliente, pra cliente parsear .dem LOCALMENTE em vez de
upload pro Railway.

Arquivo `demo_parser.py` é cópia BIT-EXACT da Railway version (commit
fe6f6c3 onwards). Sync mantido manualmente: quando Railway version mudar,
copiar arquivo de volta. Documentado em ROADMAP — ver Sprint I.5.

Por que cópia em vez de import dinâmico:
  - fragreel e fragreel-client são repos separados
  - Cliente não tem acesso ao código do server em runtime
  - Cópia explícita simplifica build (PyInstaller bundle conhece arquivos)
  - Sync manual é OK porque mudanças no parser são raras (~1× por release)

Uso:
    from local_parser.demo_parser import parse, ParsedDemo
    parsed = parse(Path("demo.dem"), player_steamid="76561198...")
"""

from .demo_parser import (
    parse,
    ParsedDemo,
    Kill,
    BombEvent,
    RoundState,
    HAS_DEMOPARSER,
)

__all__ = [
    "parse",
    "ParsedDemo",
    "Kill",
    "BombEvent",
    "RoundState",
    "HAS_DEMOPARSER",
]
