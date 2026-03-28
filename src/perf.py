from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Tuple

import fitz
from PIL import Image, ImageOps


annexure_page_cache: Dict[str, int | None] = {}
rendered_page_cache: Dict[Tuple[str, int], Image.Image] = {}
cropped_rendered_page_cache: Dict[Tuple[str, int], Image.Image] = {}
ocr_text_cache: Dict[Tuple[str, int, str], str] = {}
vision_json_cache: Dict[Tuple[str, int], Dict[str, str]] = {}


def log_timing(stage: str, elapsed_seconds: float, detail: str = "") -> None:
    if os.getenv("TIMING_LOGS", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return
    suffix = f" | {detail}" if detail else ""
    print(f"[timing] {stage}: {elapsed_seconds:.3f}s{suffix}")


def render_page_image(pdf_path: str, page_num: int, crop_margins: bool = True) -> Image.Image:
    raw_key = (pdf_path, page_num)
    if raw_key not in rendered_page_cache:
        started = time.perf_counter()
        with fitz.open(pdf_path) as document:
            if page_num < 0 or page_num >= document.page_count:
                raise ValueError(f"Invalid page number {page_num} for {pdf_path}")
            page = document[page_num]
            matrix = fitz.Matrix(150 / 72, 150 / 72)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            rendered_page_cache[raw_key] = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        log_timing("render_page", time.perf_counter() - started, f"{Path(pdf_path).name} page {page_num + 1}")

    if not crop_margins:
        return rendered_page_cache[raw_key].copy()

    if raw_key not in cropped_rendered_page_cache:
        image = rendered_page_cache[raw_key].copy()
        grayscale = image.convert("L")
        inverted = ImageOps.invert(grayscale)
        content_box = inverted.getbbox()
        if content_box:
            image = image.crop(content_box)
        cropped_rendered_page_cache[raw_key] = image
    return cropped_rendered_page_cache[raw_key].copy()
