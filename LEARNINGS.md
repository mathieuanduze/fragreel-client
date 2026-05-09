# FragReel Client — Engineering Learnings & Invariants

**Quem deve ler isso**: qualquer agent/dev abrindo este repo. Antes de mexer
em scoring/capture/cluster/editor coordination, leia. Cada bug recorrente
abaixo já foi atacado N vezes em iterações pq o contexto se perdeu entre
sessões.

> Source de verdade: este arquivo + `~/.claude/.../memory/` (regras
> permanentes do user) + Obsidian `Projetos Paralelos/FragReel/Aprendizados
> FragReel.md` (cross-cutting). Quando shipar fix de bug não-trivial, copia
> aqui (e atualiza Obsidian via regra `rule_capture_lessons`).

---

## 🔁 Bugs recorrentes — leia antes de re-implementar

### 1. Defuse cortado (4 iterações até v5.7.15)

**Sintoma**: animação de defuse no reel termina antes do "Bomba defusada"
notification CS2. Editor cuts mid-animation OR freeze frame final.

**História**:
- Round 4c Fase 1.34 (26/04): bumped REACTION_PAD 2.5s → 4.0s. Não fechou.
- Sprint v5.7 (08/05): 4.0s → 6.0s. Não fechou.
- Sprint v5.7.13 (08/05): 6.0s → 8.5s. Não fechou.
- Sprint v5.7.15 (09/05) ✅: editor 8.5s + capture buffer 5s → 9s
  COORDENADOS resolveu.

**Root cause REAL**: NÃO era só o REACTION_PAD do editor. Era a coordenação
entre 2 pontos:
1. **Source capture** (`scripts/capture_script.py V2_DEFUSE_POST_BUFFER_S`):
   quanto o cluster v2 captura DEPOIS do defuse_tick. Se 5s, .mov tem só
   `defuse_tick + 5s` de footage.
2. **Scene render** (`editor/src/theme.ts REACTION_PAD_DEFUSE_SEC`): quanto
   o editor mostra a scene depois do `bomb_action_timestamp`.

**Invariante que NÃO PODE QUEBRAR**:
```
SOURCE capture buffer >= SCENE editor reaction pad + 0.5s safety
```
Senão scene quer renderizar X seg mas source só tem Y < X seg → freeze
frame final OR cut.

**Bumps coordenados (v5.7.15)**:
- Editor: REACTION_PAD_DEFUSE_SEC = 8.5s
- Capture: V2_DEFUSE_POST_BUFFER_S = 9.0s (8.5 + 0.5 safety)

**Se Mathieu reportar de novo "defuse cortado"**: NÃO bumpe SÓ um lado.
Verifique invariante. Provavelmente cluster timing drift ou bug específico
de demo (ex: defuse ANTES de fim de cluster v2 first window).

### 2. Bomb timer ausente em CT defuse round (T plant)

**Sintoma**: barra vermelha do bomb timer não aparece em rounds onde TR
plantou e user defusou.

**Cadeia que precisa funcionar**:
1. `local_parser/demo_parser.py _parse_bomb_events`: keep events com
   `user_steamid=""` (v0.6.53 fix, NÃO drope estes)
2. Vercel scorer.ts: `bomb_planted_timestamp` loop SEM filtro steamid
   (qualquer plant no round)
3. Vercel scorer.ts `bomb_action_timestamp`: 2-tier fallback (prefer steamid
   match → fallback any matching round+action)
4. Editor `HighlightScene.tsx plantTimestamp`: 3-tier fallback
   (`bomb_planted_timestamp` → `bomb_action_timestamp` se plant_won →
   `bomb_action_timestamp - 35s` se defuse)

**INVARIANTES que não podem quebrar**:
- `bomb_planted_timestamp` no scorer NUNCA deve filtrar por player_steamid
  (quem plantou não importa, só QUE plantou)
- Parser bomb_events keep com steamid vazio normalize pra `""` (não drope)
- Editor SEMPRE checar 3 tiers, não só `bomb_planted_timestamp`

### 3. Killfeed mostra "INIMIGO" em vez do nome real

**Cadeia**:
1. `local_parser/demo_parser.py _parse_kills`: capturar `user_name` (victim,
   convenção CSGO) + `attacker_name` direto do player_death event row
   (v0.6.52)
2. `api_client.py _build_request_body`: serializar esses campos
3. Vercel scorer.ts: 3-tier `victim_name` (kill.victim_name DIRETO →
   roster lookup → null → editor mostra "Inimigo" generic)

**INVARIANTE**: NÃO depender SÓ do roster_by_steamid. Em demos com
`parse_player_info()` empty, roster vem `{}` → tudo vira "INIMIGO".
Capture name DIRETO do event row.

### 4. Roster 6v4 (não 5v5)

**Causa**: em CS2 competitivo, times trocam lados após round 12 (halftime).
Se attribution de team usar `if team is None: set team` (primeira kill),
players cuja 1ª kill foi pré-halftime ficam fixed → roster errado.

**Fix v0.6.58**: SEMPRE overwrite team com kill mais recente. all_kills é
chronológico → última iteração wins → reflete lado FINAL do player.

**Edge case**: player que só morreu (0 kills) → fallback victim_team da
PRIMEIRA death (mantém `if is None` SÓ nesse path — kills attribution é
mais confiável).

**INVARIANTE**: NÃO usar `if team is None: set` no loop de attacker.
Sempre overwrite. Ver `local_api.py:_demo_roster()` lines 745-770.

### 5. Score do HUD repetido em todos highlights

**Sintoma**: HUD top-center mostra mesmo placar (ex: 7x0) em todos os
highlights do reel, mesmo passando por rounds diferentes.

**Causa**: editor `HighlightScene.tsx` parseava `match.score` (final score
"11-3") pra todos os highlights.

**Fix v5.7.15**: scorer emite `score_ct_at_round` + `score_t_at_round` per
highlight (acumulado pré-round, loop sobre `events.rounds` com
winner_team OR fallback via user_team + user_won). Editor lê direto,
fallback pra match.score split se field missing.

**INVARIANTE**: NÃO assumir score do match aplica ao highlight. Cada
highlight tem seu próprio momento histórico no demo.

### 6. Cache stale forçava `rm -rf ~/.fragreel/matches/`

**Antes v0.6.54**: cada vez que adicionavamos campo novo no scorer (ex:
victim_name na v0.6.52), match_docs cacheados em disco ficavam STALE.
Mathieu tinha que MANUALMENTE deletar pasta.

**Fix v0.6.54+**: schema versioning. `MATCH_DOC_SCHEMA_VERSION = "vN"` em
`local_matches_store.py`. `save_match` adiciona, `load_match` valida →
return None se mismatch → web AutoReanalyze trigger → re-score.

**Política de bump (LEIA antes de mudar scorer)**:
- Mudou shape do match_doc (campo novo, rename) → BUMP
- Mudou semântica (timestamp sec → ms) → BUMP
- Bug fix mantendo shape (mais nulls populated) → na DÚVIDA bumpa
  (re-score local é barato ~5-15s)

**INVARIANTE**: SCORER_VERSION coordinated com MATCH_DOC_SCHEMA_VERSION.
Cada vez que scorer muda saída → ambos bumpam. Versão mismatch → cache
auto-invalida.

### 7. Pro demo "Demo não encontrada" loop

**Sintoma**: importa .dem HLTV, click "Mapear players" → escolhe player
→ /match/[id] dá "Demo não encontrada" loop.

**Causa**: `local_api.py /demos/<sha>/score` parseava + scoreava + retornava
match_doc, mas NUNCA chamava `local_matches_store.save_match()`. Web depois
tentava /matches/<id> → load_match return None → 404 → AutoReanalyze loop.

**Fix v0.6.55**: após `parse_and_score_locally`, chama
`save_match(match_doc.id, match_doc)`. Próximo lookup serve direto.

**INVARIANTE**: TODA endpoint que gera novo match_doc DEVE chamar
save_match imediato. Se você adicionar /demos/<x>/algumacoisa que retorna
match_doc, salve.

---

## 🚫 Coisas que ESTÃO funcionando — NÃO mude sem motivo forte

### A. Pipeline de captura HLAE
- `cs2_launcher.py` abre CS2 invisível (offscreen + mute)
- HLAE injects via `mirv_streams` pra capturar POV específico
- ProRes encode pra .mov sem alpha (v0.6.42 fix — ProRes 4444 com alpha
  causava Rust panic em Remotion compositor)
- **NÃO mude pra alpha=true sem entender o panic em scalable_frame.rs:343**

### B. Cluster v2 algorithm
- Localiza groups de kills + bomb events em windows ajustáveis
- Multi-take per highlight quando gap > MERGE_GAP (cluster múltiplos .movs)
- ffmpeg concat junta os takes
- **NÃO simplifique pra single-window assumption** — Mathieu já reportou
  "plant não aparece" quando R14 W3+W4 cluster era simplificado

### C. Local-only rendering (privacy)
- HLAE + ffmpeg + Remotion rodam 100% no PC do user
- Vídeo NUNCA sai do PC (só tag de "ad watched" pra analytics anônimo)
- LP, /matches, /privacy, hero copy reforçam isto
- **NÃO migre pra cloud render mesmo "pra performance"** — privacy é prop
  central do produto

### D. Steam OpenID single-login
- Web: login Steam OAuth via Railway redirect
- Cliente: scanner usa user_steamid pra filtrar player_kills
- Steam Web API key (STEAM_WEB_API_KEY) usada SÓ pra `/api/steam/avatars`
  (não pra match history — rejected pivot Sprint DEMO-3 v3)

### E. Scoring fields (Sprint #6.5+)
Estes campos foram adicionados após 4 sprints + diag PC. NÃO REMOVA do
schema sem coordinated bump:
- `kills[].aesthetic_score / aesthetic_style` — pra cinematic effects (atualmente unused, mantém pra futuro)
- `kills[].pov_eligible / victim_steamid / victim_name / kill_tick / distance`
  — POV cuts long-distance
- `highlights[].bomb_planted_timestamp` — INDEPENDENTE de quem plantou
- `highlights[].bomb_action_tick / bomb_action_timestamp` — quando user
  fez plant/defuse
- `highlights[].score_ct_at_round / score_t_at_round` — placar AT esse round

### F. Schema versioning policy
- `MATCH_DOC_SCHEMA_VERSION` em `local_matches_store.py`
- Bump quando: shape change, semântica change, na dúvida (re-score barato)
- v1 → v2 → v3 → v4 → v5 (cada versão tem motivo documentado no comment)

### G. Web ↔ client coordination
- Web envia POST `/demos/<sha>/score` body `{target_steamid}` → recebe match_doc
- Cliente PRECISA ter Content-Type: application/json (Sprint #7 hotfix)
- Web AutoReanalyze flow espera 404 do `/matches/<id>` pra trigger re-score

### H. AppShell sidebar 4 sections fixas
- Mathieu spec v5.4: "quero seções fixas no sidebar"
- Minhas Demos / Editar FragReel (conditional active) / Meus FragReels /
  Reportar Bug
- NÃO esconda sections quando vazio. Vazio mostra "Nenhum" estado.

### I. UX da edição: 1-click pra reel
- /matches → click "Mapear players" → expand inline com roster
- Click player → AnalyzingDemoModal (com ad space) → /match/[id]
- /match/[id] → seleciona cenas + mood → "Gerar FragReel" → AdModal →
  MP4 desktop
- Mathieu rejected redundant pages tipo "Demos Analisadas" separada,
  /demo/[sha] roster picker duplicate. Mantenha unified.

---

## 🔧 Onde encontrar coisas (cheat sheet)

| O quê | Path |
|---|---|
| Demo parser (Python) | `local_parser/demo_parser.py` |
| Scoring API call | `api_client.py score_via_api()` |
| Local match store | `local_matches_store.py` |
| Capture orchestration | `render_coordinator.py` |
| HLAE bridge | `hlae_runner.py` |
| Cluster v2 algorithm | `scripts/capture_script.py` |
| Local API HTTP | `local_api.py` |
| Vercel scorer | `../fragreel/web/app/api/score/lib/scorer.ts` |
| Editor (Remotion) | `../fragreel/editor/src/` |
| Editor types | `../fragreel/editor/src/types.ts` |
| Editor scene end logic | `../fragreel/editor/src/theme.ts` |
| Web match page | `../fragreel/web/app/match/[id]/MatchClient.tsx` |
| Web /matches list | `../fragreel/web/components/MinhasDemosClient.tsx` |

---

## 📋 Bug fix checklist (use antes de shipar)

- [ ] Bug raiz identificado (não só sintoma)?
- [ ] Tem invariante violado? Documente acima
- [ ] Fix touches scoring shape? Bump SCORER_VERSION + SCHEMA_VERSION
- [ ] Fix touches editor render? Confere coordinated invariants (defuse: source >= scene)
- [ ] Tem regressão potencial? Adicione test sentinel ou comment alertando
- [ ] Atualizou este LEARNINGS.md se foi recorrente?
- [ ] Atualizou Obsidian Aprendizados FragReel.md se foi cross-cutting?

---

## 📜 Histórico de bumps coordenados

| Data | Editor | Capture | Scorer | Schema | Trigger |
|---|---|---|---|---|---|
| 2026-04-26 1.34 | REACTION_PAD_DEFUSE 4.0s | V2_DEFUSE_POST_BUFFER 5.0s | — | — | "defuse não termina" |
| 2026-05-08 v5.7 | REACTION_PAD_DEFUSE 6.0s | (mantido 5.0s) | — | — | "defuse cortado" round 2 |
| 2026-05-08 v5.7.13 | REACTION_PAD_DEFUSE 8.5s | (mantido 5.0s) | v0.7.1 orphan attribution | v4 | "defuse cortado" round 3 |
| 2026-05-09 v5.7.15 | REACTION_PAD_DEFUSE 8.5s | **V2_DEFUSE_POST_BUFFER 9.0s** | v0.7.2 score_at_round | v5 | "defuse cortado" round 4 + score 7x0 + qualidade |

**Lição da tabela**: bump SÓ um lado é band-aid. Bump COORDINATED resolve.
Quando Mathieu reportar mesmo bug N vezes, suspeita de coordenação não
aplicada.
