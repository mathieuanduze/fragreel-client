"""
Gera splash.png usado pelo PyInstaller durante o extract do .exe.

Bug #16 (28/04): Mathieu reportou que .exe v0.4.2 abre depois de 5-15s
sem feedback visual (PyInstaller ONEFILE extrai 354MB pra %TEMP% antes
de iniciar o app). User não sabe se travou ou tá iniciando.

Solução: splash screen via PyInstaller `--splash` (configurado no
FragReel.spec via `Splash()`). main.py chama pyi_splash.close() após
boot pra fechar essa imagem e abrir a UI normal.

Roda este script:
  python generate_splash.py
Resultado: splash.png 480x360 com brand FragReel + "Carregando..."

Idempotente: rodar de novo sobrescreve. Não precisa rodar todo build —
só quando design mudar.
"""
from __future__ import annotations

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUT_PATH = Path(__file__).parent / "splash.png"
WIDTH, HEIGHT = 480, 360

# Cores alinhadas com o tema da landing fragreel.gg
BG_COLOR = (13, 13, 26)      # #0D0D1A
ACCENT = (255, 107, 53)      # #FF6B35
TEXT_PRIMARY = (232, 232, 240)  # #E8E8F0
TEXT_MUTED = (140, 140, 160)


def _try_font(name: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Tenta carregar font do sistema, fallback pra default."""
    candidates = [
        f"/System/Library/Fonts/Supplemental/{name}",       # macOS
        f"/System/Library/Fonts/{name}",                    # macOS system
        f"C:\\Windows\\Fonts\\{name}",                      # Windows
        f"/usr/share/fonts/truetype/dejavu/{name}",         # Linux
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except (OSError, IOError):
            continue
    return ImageFont.load_default()


def main() -> None:
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)

    # Glow circle atrás do logo (mimica o radial gradient da landing)
    glow_radius = 140
    glow_center = (WIDTH // 2, HEIGHT // 2 - 30)
    for r in range(glow_radius, 0, -8):
        alpha = int(20 * (1 - r / glow_radius))
        if alpha > 0:
            overlay = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
            ovd = ImageDraw.Draw(overlay)
            ovd.ellipse(
                (glow_center[0] - r, glow_center[1] - r,
                 glow_center[0] + r, glow_center[1] + r),
                fill=(*ACCENT, alpha),
            )
            img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
            draw = ImageDraw.Draw(img)

    # Logo / brand text
    title_font = _try_font("Arial Bold.ttf", 56) or _try_font("Arial.ttf", 56)
    subtitle_font = _try_font("Arial.ttf", 18)
    loading_font = _try_font("Arial.ttf", 14)

    title = "FragReel"
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_w = title_bbox[2] - title_bbox[0]
    draw.text(
        ((WIDTH - title_w) // 2, HEIGHT // 2 - 60),
        title,
        fill=ACCENT,
        font=title_font,
    )

    subtitle = "CS2 Highlights"
    sub_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
    sub_w = sub_bbox[2] - sub_bbox[0]
    draw.text(
        ((WIDTH - sub_w) // 2, HEIGHT // 2 + 10),
        subtitle,
        fill=TEXT_PRIMARY,
        font=subtitle_font,
    )

    # Loading indicator
    loading = "Carregando..."
    load_bbox = draw.textbbox((0, 0), loading, font=loading_font)
    load_w = load_bbox[2] - load_bbox[0]
    draw.text(
        ((WIDTH - load_w) // 2, HEIGHT - 60),
        loading,
        fill=TEXT_MUTED,
        font=loading_font,
    )

    # Versão / placeholder de progress bar (linha decorativa)
    bar_y = HEIGHT - 35
    bar_x_start = WIDTH // 4
    bar_x_end = WIDTH - WIDTH // 4
    draw.line([(bar_x_start, bar_y), (bar_x_end, bar_y)], fill=(45, 45, 68), width=2)
    # Pequena seção colorida (representativa, não animada — splash PyInstaller é estático)
    section_w = (bar_x_end - bar_x_start) // 3
    draw.line(
        [(bar_x_start, bar_y), (bar_x_start + section_w, bar_y)],
        fill=ACCENT,
        width=2,
    )

    img.save(OUT_PATH, "PNG", optimize=True)
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"✅ Generated {OUT_PATH} ({WIDTH}x{HEIGHT}, {size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
