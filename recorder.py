"""
FragReel Recorder — captura CS2 em background via ffmpeg (gdigrab).

Fluxo:
  1. CS2 inicia → start() → ffmpeg grava a janela em segmentos de 5 min
  2. Partida termina → .dem aparece → watcher sobe para a API
  3. API retorna timestamps dos highlights
  4. extract_clips(highlights, match_duration) → clipa os momentos exatos
  5. Clips salvos em ~/Videos/FragReel/{match_id}/
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger("fragreel.recorder")

# ── Constantes ────────────────────────────────────────────────────────────────

CS2_WINDOW_TITLE = "Counter-Strike 2"

CAPTURE_FPS     = 60
CAPTURE_CRF     = 23          # qualidade: 18=alta, 28=baixa. 23 = bom equilíbrio
CAPTURE_PRESET  = "ultrafast" # mais rápido de codificar (arquivo maior)
CAPTURE_SCALE   = "1920:1080"

SEGMENT_SECONDS = 300         # 5 min por segmento
SEGMENTS_KEEP   = 14          # manter últimos 70 min de gravação

PRE_ROLL_SEC    = 6.0         # segundos antes do highlight no clipe
POST_ROLL_SEC   = 3.0         # segundos depois


class Recorder:
    """Grava a janela do CS2 em segmentos rotativos e extrai clipes nos timestamps certos."""

    def __init__(self, base_dir: Optional[Path] = None):
        if base_dir is None:
            base_dir = Path.home() / "Videos" / "FragReel" / "_sessions"
        self._base_dir    = base_dir
        self._session_dir : Optional[Path]     = None
        self._start_time  : Optional[datetime] = None
        self._proc        : Optional[subprocess.Popen] = None
        self._lock        = threading.Lock()
        self._running     = False
        self._cleaner_thread: Optional[threading.Thread] = None

    # ── API pública ───────────────────────────────────────────────────────────

    @property
    def is_recording(self) -> bool:
        return self._running and self._proc is not None and self._proc.poll() is None

    def start(self) -> bool:
        """Inicia gravação. Retorna True se iniciou, False se já gravava ou sem ffmpeg."""
        with self._lock:
            if self._running:
                log.info("Recorder já está ativo.")
                return False
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._session_dir = self._base_dir / ts
            self._session_dir.mkdir(parents=True, exist_ok=True)
            self._start_time  = datetime.now()

        seg_pattern = str(self._session_dir / "seg_%05d.mp4")
        cmd = self._build_ffmpeg_cmd(seg_pattern)

        try:
            kwargs: dict = dict(
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if os.name == "nt":
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]

            self._proc    = subprocess.Popen(cmd, **kwargs)
            self._running = True
            log.info(f"Recorder iniciado (PID {self._proc.pid}) → {self._session_dir}")

            # Thread que limpa segmentos antigos
            self._cleaner_thread = threading.Thread(
                target=self._cleaner_loop, daemon=True, name="seg-cleaner"
            )
            self._cleaner_thread.start()
            return True

        except FileNotFoundError:
            log.error("ffmpeg não encontrado. Verifique se está na pasta do client.")
            return False
        except Exception as e:
            log.error(f"Erro ao iniciar recorder: {e}")
            return False

    def stop(self) -> None:
        """Para a gravação graciosamente."""
        with self._lock:
            if not self._running:
                return
            self._running = False

        if self._proc and self._proc.poll() is None:
            log.info("Parando recorder...")
            try:
                self._proc.stdin.write(b"q\n")
                self._proc.stdin.flush()
                self._proc.wait(timeout=12)
            except Exception:
                self._proc.kill()
            log.info("Recorder parado.")

    def extract_clips(
        self,
        highlights: list[dict],
        match_duration_sec: float,
        output_dir: Path,
    ) -> list[Path]:
        """
        Extrai clipes de vídeo para cada highlight.

        highlights       : lista de dicts com 'rank', 'start', 'end' (segundos relativos à demo)
        match_duration_sec: duração estimada da partida (max timestamp de kill)
        output_dir       : pasta onde os clipes serão salvos
        """
        if not self._session_dir or not self._start_time:
            log.warning("extract_clips: nenhuma sessão de gravação ativa.")
            return []

        # Quanto tempo de gravação passou desde que o CS2 abriu
        rec_elapsed = (datetime.now() - self._start_time).total_seconds()

        # Offset dentro da gravação onde a partida começou
        # (gravação começa quando CS2 abre, partida começa depois)
        # Adicionamos 30s de margem para menus de início
        match_start_in_rec = max(0.0, rec_elapsed - match_duration_sec - 30.0)

        log.info(
            f"extract_clips: gravado={rec_elapsed:.0f}s  "
            f"partida≈{match_duration_sec:.0f}s  "
            f"offset={match_start_in_rec:.0f}s"
        )

        output_dir.mkdir(parents=True, exist_ok=True)
        clips: list[Path] = []

        for h in highlights:
            clip_start = match_start_in_rec + h["start"] - PRE_ROLL_SEC
            clip_start = max(0.0, clip_start)
            duration   = (h["end"] - h["start"]) + PRE_ROLL_SEC + POST_ROLL_SEC
            out        = output_dir / f"highlight_{h['rank']:02d}.mp4"

            if self._cut_clip(clip_start, duration, out):
                clips.append(out)

        log.info(f"{len(clips)}/{len(highlights)} clipes extraídos em {output_dir}")
        return clips

    # ── Internos ──────────────────────────────────────────────────────────────

    def _build_ffmpeg_cmd(self, seg_pattern: str) -> list[str]:
        return [
            "ffmpeg",
            "-loglevel", "warning",
            # Captura a janela do CS2 pelo título
            "-f", "gdigrab",
            "-framerate", str(CAPTURE_FPS),
            "-i", f"title={CS2_WINDOW_TITLE}",
            # Vídeo
            "-vf", f"scale={CAPTURE_SCALE}",
            "-c:v", "libx264",
            "-preset", CAPTURE_PRESET,
            "-crf", str(CAPTURE_CRF),
            "-pix_fmt", "yuv420p",
            # Saída segmentada
            "-f", "segment",
            "-segment_time", str(SEGMENT_SECONDS),
            "-reset_timestamps", "1",
            "-strftime", "0",
            seg_pattern,
        ]

    def _cut_clip(self, start_sec: float, duration: float, output: Path) -> bool:
        """Corta um clipe dos segmentos gravados."""
        segs = sorted(self._session_dir.glob("seg_*.mp4"))  # type: ignore[union-attr]
        if not segs:
            log.warning("_cut_clip: nenhum segmento disponível.")
            return False

        first_seg_idx = int(start_sec // SEGMENT_SECONDS)
        last_seg_idx  = int((start_sec + duration) // SEGMENT_SECONDS)
        needed        = segs[first_seg_idx : last_seg_idx + 1]

        if not needed:
            log.warning(f"_cut_clip: segmentos {first_seg_idx}-{last_seg_idx} não encontrados")
            return False

        offset_in_seg = start_sec - (first_seg_idx * SEGMENT_SECONDS)

        if len(needed) == 1:
            cmd = [
                "ffmpeg", "-y", "-loglevel", "warning",
                "-ss", f"{offset_in_seg:.3f}",
                "-i", str(needed[0]),
                "-t", f"{duration:.3f}",
                "-c", "copy",
                str(output),
            ]
        else:
            concat_file = self._session_dir / f"concat_{output.stem}.txt"  # type: ignore[union-attr]
            concat_file.write_text(
                "\n".join(f"file '{s.resolve()}'" for s in needed),
                encoding="utf-8",
            )
            cmd = [
                "ffmpeg", "-y", "-loglevel", "warning",
                "-f", "concat", "-safe", "0",
                "-i", str(concat_file),
                "-ss", f"{offset_in_seg:.3f}",
                "-t", f"{duration:.3f}",
                "-c", "copy",
                str(output),
            ]

        extra: dict = {}
        if os.name == "nt":
            extra["creationflags"] = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]

        try:
            r = subprocess.run(cmd, capture_output=True, timeout=60, **extra)
            if r.returncode == 0:
                kb = output.stat().st_size // 1024
                log.info(f"Clipe salvo: {output.name} ({kb} KB)")
                return True
            log.error(f"ffmpeg clip falhou: {r.stderr.decode(errors='replace')[:300]}")
        except Exception as e:
            log.error(f"Erro ao cortar clipe: {e}")
        return False

    def _cleaner_loop(self) -> None:
        """Apaga segmentos antigos a cada minuto para liberar disco."""
        while self._running:
            time.sleep(60)
            if not self._session_dir:
                continue
            segs = sorted(self._session_dir.glob("seg_*.mp4"))
            for old in segs[:-SEGMENTS_KEEP]:
                try:
                    old.unlink()
                    log.debug(f"Segmento deletado: {old.name}")
                except Exception:
                    pass
