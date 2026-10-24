#!/usr/bin/env python3
"""
dataset-builder: Automated Dataset Creation & Indexing Engine

Transforms any folder of documents, PDFs, manuals, text files, or data into
a production-grade, dual-access dataset compatible with:
1. Producer Pal the APP (Live Actions Helper / Genius) via library_search and library_read
2. AI Skills (producer-pal-genius, ai-database-browser, Claude, Grok, Antigravity) via SQLite FTS5 and Parquet.

MANDATORY RULE: Destination directory MUST be specified first before building.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

try:
    import pymupdf as fitz
    HAS_FITZ = True
except ImportError:
    try:
        import fitz
        HAS_FITZ = True
    except ImportError:
        HAS_FITZ = False


# ── Utilities ──────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    """Convert text to a filesystem and SQL safe slug."""
    text = re.sub(r"[^\w\s-]", "", text.strip().lower())
    text = re.sub(r"[-\s]+", "_", text)
    return text[:80] or "doc"


def doc_rel_dir(doc: dict) -> str:
    """Tier-3 markdown directory for a document: category/subcategory when the
    subcategory is genuine (present and distinct from category), else just
    category. Mirrors the B:\\DATABASES Phase 3/4 convention: a subcategory
    that duplicates its category is a no-op value, not a real second tier,
    and must not produce a fake folder level."""
    cat = doc["category"]
    subcat = doc.get("subcategory")
    if subcat and subcat != cat:
        return f"{cat}/{subcat}"
    return cat


def extract_keywords(text: str, top_n: int = 10) -> list[str]:
    """Extract salient keywords from text for tagging and FTS boosting."""
    stopwords = {
        "the", "and", "is", "in", "to", "of", "a", "for", "with", "on", "at",
        "by", "from", "up", "about", "into", "over", "after", "this", "that",
        "it", "an", "be", "as", "or", "are", "from", "was", "will", "can",
        "has", "have", "had", "not", "but", "what", "all", "were", "when",
        "we", "there", "been", "which", "their", "more", "if", "out", "also"
    }
    words = re.findall(r"\b[a-zA-Z]{3,}\b", text.lower())
    freq: dict[str, int] = {}
    for w in words:
        if w not in stopwords:
            freq[w] = freq.get(w, 0) + 1
    sorted_words = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return [w for w, _ in sorted_words[:top_n]]


def extract_text_from_file(path: Path) -> tuple[str, int]:
    """Extract clean text and page count from a file (PDF, MD, TXT, JSON)."""
    suffix = path.suffix.lower()
    text = ""
    pages = 1

    if suffix == ".pdf":
        if HAS_FITZ:
            try:
                doc = fitz.open(str(path))
                pages = len(doc)
                chunks = [p.get_text() or "" for p in doc]
                text = "\n\n".join(chunks)
            except Exception:
                pass
        if not text and HAS_PYPDF:
            try:
                reader = PdfReader(str(path))
                pages = len(reader.pages)
                chunks = []
                for p in reader.pages:
                    t = p.extract_text() or ""
                    chunks.append(t)
                text = "\n\n".join(chunks)
            except Exception:
                pass
    elif suffix in {".md", ".txt", ".json", ".yaml", ".yml", ".csv", ".tsv", ".html", ".xml"}:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            text = text.replace("\ufffd", " - ")
            word_count = len(text.split())
            pages = max(1, word_count // 350)
        except Exception:
            pass

    return text.strip(), pages


# ── Domain Presets ─────────────────────────────────────────────────────────────

DOMAIN_PRESETS = {
    "music": {
        "scope": "Live Actions OSC & Producer Pal Integration",
        "capabilities": [
            "Native Ableton Live 12 device instantiation and parameter setting (Drift, Analog, Wavetable, Simpler, Sampler, Drum Rack, EQ Eight, Compressor, Glue Compressor, Auto Filter, Reverb, Delay).",
            "Session view MIDI clip creation with chord progressions, basslines, melodies, and drum grooves (`live_write_notes`).",
            "Live Set global management: tempo, time signature, scale/mode root, locators, track creation, and mixer routings.",
            "Deterministic key/mode detection via Krumhansl-Schmuckler pitch-class correlation (`live_detect_key`).",
        ],
        "limitations": [
            "Third-party VST/AU plugins require cited handoff unless already present on a track.",
            "Clip automation envelopes and AI audio stem separation stay manual user actions.",
        ],
        "example_action": {
            "title": "Insert Native Drift Synth with Warm Analog Bass Settings",
            "code": 'live_create_device(track_index=0, device_name="Drift", params=[\n    {"name": "Osc 1 Wave", "value": "Saw"},\n    {"name": "Osc 2 Wave", "value": "Triangle"},\n    {"name": "Osc 2 Detune", "value": "0.12"},\n    {"name": "Filt 1 Type", "value": "24 Low"},\n    {"name": "Filt 1 Freq", "value": "280 Hz"}\n])'
        }
    },
    "m4l": {
        "scope": "Max for Live Device Architecture & LOM Access",
        "capabilities": [
            "Inspect and automate Max for Live devices via device paths (e.g. 't0/d0', 't1/d0/c0').",
            "Set parameters and read parameter values on any active M4L audio effect, MIDI effect, or instrument.",
            "Execute direct Live Object Model (LOM) calls via 'live_api'.",
            "Monitor device CPU consumption, latency, and routing paths.",
        ],
        "limitations": [
            "Cannot open the Max graphical editor window programmatically over OSC.",
            "Cannot dynamically compile or build new .amxd patcher files from scratch over OSC.",
        ],
        "example_action": {
            "title": "Inspect Parameters on Track 0 M4L Device",
            "code": 'live_read_device(track_index=0, device_index=0, include=["params", "param-values"])'
        }
    },
    "windows_and_midi_control": {
        "scope": "Windows MIDI Services (WinMM / UMP) & Live Actions MIDI Routing",
        "capabilities": [
            "Query and route MIDI between Ableton Live tracks, loopback pairs, and external hardware endpoints.",
            "Inspect MIDI clip notes, velocities, and timing over OSC (`live_read_clip`).",
            "Generate and inject test MIDI note streams and controller data directly into active tracks (`live_write_notes`).",
            "Arm tracks and manage Live Set track monitor states ('In', 'Auto', 'Off') to route inter-app MIDI.",
            "Leverage Windows MIDI Services loopback endpoints for inter-app DAW-to-DAW or tool-to-DAW routing.",
        ],
        "limitations": [
            "Modifying Windows registry keys (`Drivers32`) or installing MSI preview builds requires Windows administrator privileges.",
            "Starting, stopping, or configuring the background Windows MIDI Service (`midisrv`) requires the Windows MIDI Console or PowerShell cmdlets.",
            "Direct USB-MIDI-to-UMP translation and low-level kernel streaming driver buffers are handled by the Windows OS kernel.",
        ],
        "example_action": {
            "title": "Create Test MIDI Clip to Verify Loopback Routing",
            "code": 'live_write_notes(track_index=0, clip_index=0, notes=[\n    {"pitch": 60, "start_time": 0.0, "duration": 1.0, "velocity": 100},\n    {"pitch": 64, "start_time": 1.0, "duration": 1.0, "velocity": 100},\n    {"pitch": 67, "start_time": 2.0, "duration": 1.0, "velocity": 100},\n    {"pitch": 72, "start_time": 3.0, "duration": 1.0, "velocity": 100}\n])'
        }
    },
    "sonic_pi": {
        "scope": "Sonic Pi Live Coding Engine, Ruby Music DSL & SuperCollider Synthesis",
        "capabilities": [
            "Algorithmic live coding music composition via Ruby DSL (`live_loop`, `synth`, `sample`, `play`, `sleep`).",
            "Studio FX chains with dynamic real-time parameter sliding (`with_fx :reverb`, `control`, `slide_opts`).",
            "Deterministic and pseudo-random pattern generation using seeds, rings, ticks, and scale/chord tables (`use_random_seed`, `ring`, `tick`).",
            "Bidirectional MIDI routing with DAWs (Ableton Live) and hardware synths via virtual loopback ports (`midi`, `midi_note_on`, `midi_cc`).",
            "Networked OSC messaging and thread state synchronization across instances and devices (`osc`, `get`, `set`, `sync`).",
        ],
        "limitations": [
            "Sonic Pi scripts execute within the Sonic Pi engine runtime or via its OSC daemon listen port.",
            "SuperCollider SynthDefs must be compiled and loaded into the Sonic Pi synthesis engine.",
        ],
        "example_action": {
            "title": "Sonic Pi Generative Ambient Pulse Loop with MIDI Out to DAW",
            "code": 'live_loop :ambient_pulse do\n  use_synth :prophet\n  use_random_seed 4242\n  notes = scale(:e3, :minor_pentatonic, num_octaves: 2).shuffle\n  \n  with_fx :echo, phase: 0.375, decay: 4, mix: 0.4 do\n    with_fx :reverb, room: 0.85, mix: 0.5 do\n      4.times do\n        n = notes.tick\n        play n, release: 0.6, cutoff: rrand(70, 110), amp: 0.8\n        midi n, channel: 1, vel: 90, sustain: 0.5\n        sleep 0.5\n      end\n    end\n  end\nend'
        }
    },
    "infinite_crate": {
        "scope": "The Infinite Crate (Lyria RealTime VST) & Ableton Live Resampling",
        "capabilities": [
            "Live Set global tempo and scale synchronization to lock generative Lyria streams to DAW project timing (`live_set_tempo`, `live_set_scale`).",
            "Post-mixer audio routing from The Infinite Crate track into dedicated recording audio tracks for live loop sampling and slicing.",
            "Track monitoring and arm management for interactive jam sessions with live instrumentalists.",
            "Stem track muting and soloing to prevent frequency clash during live bass, drum, or vocal accompaniment.",
            "Native Ableton Live device chains (EQ Eight, Compressor, Saturator) for shaping generative output.",
        ],
        "limitations": [
            "Lyria RealTime prompt weighting and macro dials (TopK, Temp, Density, Bright) run inside the VST3 webview and cloud Gemini API.",
            "Cloud generation has a 10-minute continuous streaming limit before requiring a manual Reset click in the plugin UI.",
            "Prompt adjustments exhibit ~1-second network latency as generative audio buffers settle.",
        ],
        "example_action": {
            "title": "Configure Live Set Tempo, Scale, and Sample Capture Track for The Infinite Crate",
            "code": '# 1. Synchronize project tempo and harmonic key with The Infinite Crate\n'
                    'live_set_tempo(tempo=120.0)\n'
                    'live_set_scale(root="C", scale="Minor")\n\n'
                    '# 2. Track routing architecture in Ableton Live:\n'
                    '#    Track 0: "The Infinite Crate" (Generative VST3 audio source)\n'
                    '#    Track 1: "Sample Capture" (Audio From -> "1-The Infinite Crate" Post Mixer, Monitor: In)\n'
                    '# 3. Create a harmonic guide clip on Track 2 to jam along:\n'
                    'live_write_notes(track_index=2, clip_index=0, notes=[\n'
                    '    {"pitch": 48, "start_time": 0.0, "duration": 2.0, "velocity": 90},\n'
                    '    {"pitch": 51, "start_time": 2.0, "duration": 2.0, "velocity": 85},\n'
                    '    {"pitch": 55, "start_time": 4.0, "duration": 2.0, "velocity": 90},\n'
                    '    {"pitch": 58, "start_time": 6.0, "duration": 2.0, "velocity": 85}\n'
                    '])'
        }
    },
    "the_infinite_crate": {
        "scope": "The Infinite Crate (Lyria RealTime VST) & Ableton Live Resampling",
        "capabilities": [
            "Live Set global tempo and scale synchronization to lock generative Lyria streams to DAW project timing (`live_set_tempo`, `live_set_scale`).",
            "Post-mixer audio routing from The Infinite Crate track into dedicated recording audio tracks for live loop sampling and slicing.",
            "Track monitoring and arm management for interactive jam sessions with live instrumentalists.",
            "Stem track muting and soloing to prevent frequency clash during live bass, drum, or vocal accompaniment.",
            "Native Ableton Live device chains (EQ Eight, Compressor, Saturator) for shaping generative output.",
        ],
        "limitations": [
            "Lyria RealTime prompt weighting and macro dials (TopK, Temp, Density, Bright) run inside the VST3 webview and cloud Gemini API.",
            "Cloud generation has a 10-minute continuous streaming limit before requiring a manual Reset click in the plugin UI.",
            "Prompt adjustments exhibit ~1-second network latency as generative audio buffers settle.",
        ],
        "example_action": {
            "title": "Configure Live Set Tempo, Scale, and Sample Capture Track for The Infinite Crate",
            "code": '# 1. Synchronize project tempo and harmonic key with The Infinite Crate\n'
                    'live_set_tempo(tempo=120.0)\n'
                    'live_set_scale(root="C", scale="Minor")\n\n'
                    '# 2. Track routing architecture in Ableton Live:\n'
                    '#    Track 0: "The Infinite Crate" (Generative VST3 audio source)\n'
                    '#    Track 1: "Sample Capture" (Audio From -> "1-The Infinite Crate" Post Mixer, Monitor: In)\n'
                    '# 3. Create a harmonic guide clip on Track 2 to jam along:\n'
                    'live_write_notes(track_index=2, clip_index=0, notes=[\n'
                    '    {"pitch": 48, "start_time": 0.0, "duration": 2.0, "velocity": 90},\n'
                    '    {"pitch": 51, "start_time": 2.0, "duration": 2.0, "velocity": 85},\n'
                    '    {"pitch": 55, "start_time": 4.0, "duration": 2.0, "velocity": 90},\n'
                    '    {"pitch": 58, "start_time": 6.0, "duration": 2.0, "velocity": 85}\n'
                    '])'
        }
    },
    "general": {
        "scope": "General Knowledge & Data Extraction",
        "capabilities": [
            "Full-text search with Porter stemming and BM25 relevance ranking.",
            "Modular markdown chapters (<15,000 characters) for low-token LLM viewing.",
            "Direct SQL connectivity to SQLite FTS5 virtual tables.",
        ],
        "limitations": [
            "Domain-specific actions depend on the target software.",
        ],
        "example_action": {
            "title": "Query Dataset via CLI",
            "code": 'python search.py "target query" --limit 5'
        }
    }
}


# ── Core Dataset Builder Class ─────────────────────────────────────────────────

class DatasetBuilder:
    def __init__(
        self,
        source_dir: Path | None,
        dest_dir: Path,
        name: str | None = None,
        description: str | None = None,
        domain: str = "music",
        chunk_words: int = 500,
        chunk_overlap: int = 50,
        add_dir: Path | None = None,
    ):
        self.source_dir = source_dir.resolve() if source_dir else None
        self.add_dir = add_dir.resolve() if add_dir else None
        self.dest_dir = dest_dir.resolve()
        self.name = name or self.dest_dir.name
        self.safe_name = slugify(self.name)
        self.description = description or f"Structured reference knowledgebase for {self.name}."
        self.domain = domain.lower() if domain in DOMAIN_PRESETS else "music"
        self.chunk_words = chunk_words
        self.chunk_overlap = chunk_overlap

        # Auto-detect safe_name from existing dataset files if adding
        if self.add_dir and not name:
            existing_jsonls = list(self.dest_dir.glob("*_docs.jsonl"))
            if existing_jsonls:
                self.safe_name = existing_jsonls[0].name.replace("_docs.jsonl", "")

        # Auto-preserve existing description if present
        if (self.dest_dir / "README.md").exists() and not description:
            try:
                readme_lines = [l.strip() for l in (self.dest_dir / "README.md").read_text(encoding="utf-8").splitlines() if l.strip()]
                if len(readme_lines) >= 2 and not readme_lines[1].startswith("#") and not readme_lines[1].startswith("-"):
                    self.description = readme_lines[1]
            except Exception:
                pass

        self.docs: list[dict[str, Any]] = []
        self.chunks: list[dict[str, Any]] = []
        self.categories: dict[str, list[dict[str, Any]]] = {}
        self.new_docs: list[dict[str, Any]] = []
        # Single shared uid tracker across every extraction path (regular
        # files, multi-section splits, jsonl records). Each path used to
        # keep its own local seen_uids dict rebuilt/scoped differently, so a
        # base_uid claimed by one path (e.g. a multi-section PDF split)
        # could silently collide with one computed later by another path
        # (e.g. a regular single-file doc), producing a duplicate doc_uid
        # that crashes the documents.doc_uid UNIQUE constraint on insert.
        self._used_doc_uids: set[str] = set()

    def _make_unique_uid(self, base_uid: str) -> str:
        """Return base_uid if unused, else base_uid_2, _3, ... -- checked
        against the one shared tracker so no two extraction paths can ever
        hand out the same doc_uid."""
        if base_uid not in self._used_doc_uids:
            self._used_doc_uids.add(base_uid)
            return base_uid
        n = 2
        while f"{base_uid}_{n}" in self._used_doc_uids:
            n += 1
        uid = f"{base_uid}_{n}"
        self._used_doc_uids.add(uid)
        return uid

    def validate(self):
        """Ensure source exists and destination is specified."""
        if not self.source_dir.exists():
            raise FileNotFoundError(f"Source directory does not exist: {self.source_dir}")
        if not self.dest_dir:
            raise ValueError("Destination directory MUST be specified first before building.")
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        (self.dest_dir / "markdown").mkdir(exist_ok=True)
        (self.dest_dir / "xml_presets").mkdir(exist_ok=True)
        # Copy source images if present
        if self.source_dir and self.source_dir.is_dir():
            img_src = self.source_dir / "images"
            if img_src.exists() and img_src.is_dir():
                import shutil
                dest_img = self.dest_dir / "images"
                if not dest_img.exists():
                    shutil.copytree(img_src, dest_img)
                md_img = self.dest_dir / "markdown" / "images"
                if not md_img.exists():
                    shutil.copytree(img_src, md_img)

    def scan_and_extract(self):
        """Scan source directory recursively and extract documents."""
        print(f"[*] Scanning source: {self.source_dir}")
        valid_exts = {".pdf", ".md", ".txt", ".json", ".jsonl", ".yaml", ".yml", ".html", ".xml"}
        if self.source_dir.is_file():
            source_files = [self.source_dir] if self.source_dir.suffix.lower() in valid_exts else []
        else:
            source_files = [p for p in self.source_dir.glob("**/*") if p.is_file() and p.suffix.lower() in valid_exts]

        # Ignore hidden, cache, uninstaller, and scrape manifest files relative to source
        clean_files = []
        for p in source_files:
            rel_parts = p.relative_to(self.source_dir).parts if self.source_dir.is_dir() else (p.name,)
            if any(part.startswith(".") or part == "__pycache__" for part in rel_parts):
                continue
            if p.name.lower().startswith("unins"):
                continue
            if p.name.lower() in ("manifest.json", "package-lock.json", "manifest.jsonl"):
                continue
            # Skip companion rendered PDF if source markdown is co-located
            if p.suffix.lower() == ".pdf" and ((p.parent / "guide.md").exists() or (p.parent / f"{p.stem}.md").exists()):
                continue
            clean_files.append(p)

        print(f"[*] Found {len(clean_files)} candidate source files.")
        doc_id = 1
        seen_uids: dict[str, int] = {}
        seen_titles: dict[str, int] = {}
        seen_filenames: dict[str, int] = {}

        for p in clean_files:
            if p.suffix.lower() == ".jsonl":
                doc_id = self._extract_jsonl_file(p, doc_id)
                continue

            # Support multi-chapter markdown guides (e.g. guide-capture output with --- delimiters or ^# headings)
            if p.suffix.lower() == ".md":
                raw_text, _ = extract_text_from_file(p)
                parts = []
                if "\n---\n" in raw_text or "\n\n---\n\n" in raw_text:
                    parts = [pt.strip() for pt in re.split(r"\n\s*---\s*\n", raw_text) if pt.strip()]
                elif len(re.findall(r"^#\s+[^\n]+", raw_text, re.MULTILINE)) >= 3:
                    parts = [pt.strip() for pt in re.split(r"\n(?=#\s+)", raw_text) if pt.strip()]

                h_count = sum(1 for pt in parts if re.search(r"^#{1,3}\s+", pt, re.MULTILINE))
                if len(parts) >= 3 and h_count >= 3:
                    doc_id = self._extract_multi_section_markdown(p, parts, doc_id)
                    continue

            rel = p.relative_to(self.source_dir) if self.source_dir.is_dir() else Path(p.name)
            cat, subcat = self._resolve_category(rel)

            text, pages = extract_text_from_file(p)
            if not text or len(text.strip()) < 50:
                continue

            words = len(text.split())

            raw_stem = p.stem
            raw_stem = re.sub(r"^\d+[\s\-_]+", "", raw_stem)
            raw_stem = re.sub(r"\s*-\s*(Windows MIDI Services|Windows MIDI and Music dev|Windows Experience Blog)\s*$", "", raw_stem, flags=re.IGNORECASE)
            raw_stem = re.sub(r"[\s\-_]+-\s*Wind$", "", raw_stem, flags=re.IGNORECASE)
            raw_stem = raw_stem.replace("·", "-").replace("_", " ").replace("-", " ").strip()
            raw_stem = re.sub(r"\s+", " ", raw_stem)
            clean_title = raw_stem
            clean_title = re.sub(r"\bmidi\b", "MIDI", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bjson\b", "JSON", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bump\b", "UMP", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\basio\b", "ASIO", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\busb\b", "USB", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bdns\b", "DNS", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bsdk\b", "SDK", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\barm64\b", "Arm64", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bapi\b", "API", clean_title, flags=re.IGNORECASE)

            h1_match = None
            if p.suffix.lower() == ".md":
                h1_match = re.search(r"^#\s+([^\n]+)", text, re.MULTILINE)
                if h1_match and not any(h1_match.group(1).strip().lower().startswith(x) for x in ["source:", "http", "now loading"]):
                    clean_title = h1_match.group(1).strip()

            clean_title = clean_title.replace("\ufffd", " - ").replace("–", "-").replace("—", "-").strip()
            if "_" in clean_title and clean_title.isupper():
                clean_title = clean_title.replace("_", " ").title()

            if clean_title in seen_titles:
                seen_titles[clean_title] += 1
                clean_title = f"{clean_title} (Part {seen_titles[clean_title]})"
            else:
                seen_titles[clean_title] = 1

            if p.stem.lower() in ("guide", "doc", "manual") and len(rel.parts) > 1:
                clean_stem = slugify(p.parent.name)
            elif p.stem.lower() == "guide" and len(rel.parts) == 1:
                clean_stem = slugify(clean_title) if h1_match else "guide"
            else:
                clean_stem = slugify(re.sub(r"^\d+[\s\-_]+", "", p.stem))

            if clean_stem.startswith(f"{cat}_"):
                base_uid = clean_stem
            else:
                base_uid = f"{cat}_{clean_stem}"

            doc_uid = self._make_unique_uid(base_uid)

            if p.stem.lower() in ("guide", "doc", "manual") and len(rel.parts) > 1:
                target_filename = f"{slugify(p.parent.name)}.md"
            elif p.suffix.lower() == ".md":
                target_filename = f"{clean_stem}.md"
            else:
                target_filename = f"{doc_uid}.md"

            cat_file_key = f"{cat}/{target_filename}"
            if cat_file_key in seen_filenames:
                target_filename = f"{doc_uid}.md"
            seen_filenames[cat_file_key] = 1

            doc_entry = {
                "id": doc_id,
                "doc_uid": doc_uid,
                "title": clean_title,
                "category": cat,
                "subcategory": subcat,
                "source_file": rel.as_posix(),
                "relative_source": rel.as_posix(),
                "file_type": p.suffix.lower().lstrip("."),
                "filename": target_filename,
                "page_count": pages,
                "word_count": words,
                "char_count": len(text),
                "summary": " ".join(text.split()[:80]),
                "raw_text": text
            }
            self.docs.append(doc_entry)
            self.categories.setdefault(cat, []).append(doc_entry)
            doc_id += 1

        print(f"[+] Successfully extracted {len(self.docs)} documents across {len(self.categories)} categories.")

    def _resolve_category(self, rel: Path) -> tuple[str, str]:
        """Domain-aware intelligent category resolution for nested folder structures."""
        parts = [p.lower() for p in rel.parts]
        path_str = "/".join(parts)

        # Specific Windows and MIDI Services keyword resolution
        if self.safe_name in ("windows_and_midi_control", "windows_midi", "midi_control", "windows_midi_services") or "midi" in self.safe_name or "windows" in self.safe_name:
            if any(k in path_str for k in ['transport', 'loopback', 'virtual device', 'firewall', 'network midi', 'config json']):
                return 'transports_and_routing', 'transports'
            if any(k in path_str for k in ['console', 'diagnostics', 'multicast', 'registry fixer', 'settings app', 'monitor', 'powershell', 'enumeration', 'clock', 'service command', 'report', 'midiksinfo']):
                return 'midi_console_and_tools', 'tools'
            if any(k in path_str for k in ['troubleshooting', 'error', 'workaround', 'limit', 'drivers32', 'plugin may not load', 'known issues']):
                return 'troubleshooting_and_drivers', 'troubleshooting'
            if any(k in path_str for k in ['ump', 'backward', 'compatibility', 'implementation', 'translation', 'device ids', 'arm64', 'about windows midi services']):
                return 'architecture_and_ump', 'architecture'
            if any(k in path_str for k in ['announce', 'preview', 'releases', 'boost in windows', 'system requirements', 'update', 'repo']):
                return 'announcements_and_releases', 'releases'

        # Specific M4L keyword resolution
        if self.safe_name in ("m4l", "max_for_live"):
            if any(k in path_str for k in ['error', 'latency', 'preferences', 'stop working', 'could not be loaded', 'unable to undo', 'display problems', 'saving disabled', 'edit button']):
                return 'troubleshooting_and_errors', 'errors'
            if any(k in path_str for k in ['best practice', 'supporting older']):
                return 'best_practices', 'standards'
            if any(k in path_str for k in ['masterclass', 'how to build', 'fundamentals', 'gen patcher', 'jitter']):
                return 'tutorials_and_guides', 'tutorials'
            if any(k in path_str for k in ['specific objects', 'api', 'sysex']):
                return 'objects_and_api', 'lom_api'
            if any(k in path_str for k in ['cookbook', 'learning resources', 'support', 'devices in live']):
                return 'resources_and_reference', 'reference'

        # Specific Novation directory resolution
        if self.safe_name == "novation":
            cat = slugify(rel.parts[0]) if len(rel.parts) > 1 else "general"
            subcat = slugify(rel.parts[1]) if len(rel.parts) > 2 else cat
            return cat, subcat

        # Specific Fall 2026 Research resolution
        if (self.add_dir and "new research fall 2026" in str(self.add_dir).lower()) or (self.source_dir and "new research fall 2026" in str(self.source_dir).lower()) or "new research fall 2026" in path_str:
            cat = "new_research_fall_2026"
            if any(k in path_str for k in ['wavetable', 'frequency modulation', 'pitch-bending risers', 'modal resonator', 'resampling and glitch', 'per-note randomization']):
                return cat, 'synthesis_and_sound_design'
            if any(k in path_str for k in ['transient', 'percussive alignment']):
                return cat, 'transient_shaping_and_alignment'
            if any(k in path_str for k in ['mid-side', 'phase', 'haas', 'width', 'spatial', 'attenuation', 'panning', 'phaser-flanger']):
                return cat, 'spatial_stereo_and_phase'
            if any(k in path_str for k in ['distortion', 'saturation', 'wow', 'tape', 'harmonic layering', 'pitch drift', 'audio-rate modulation']):
                return cat, 'saturation_and_tape_color'
            if any(k in path_str for k in ['gain staging', 'loudness', 'sidechain', 'compression', 'dynamics']):
                return cat, 'dynamics_and_gain_staging'
            if any(k in path_str for k in ['spectral', 'equalization', 'filtering', 'frequency bracketing', 'low-end']):
                return cat, 'spectral_and_eq'
            if any(k in path_str for k in ['groove', 'arrangement', 'psychoacoustic', 'void', 'tension', 'automation as a performance', 'call-and-response']):
                return cat, 'arrangement_and_psychoacoustics'
            if any(k in path_str for k in ['workflow', 'referencing', 'template', 'digital signal processing', 'mastering']):
                return cat, 'mastering_and_workflow'
            return cat, 'general_production'

        if self.domain == "music":
            if "manual" in path_str:
                return "manual", "live_12_manual"
            if "max for live" in path_str or "maxdevtools" in path_str or "m4l" in path_str:
                return "max_for_live", "m4l_tools"
            if "link and cloud" in path_str:
                return "link_and_cloud", "cloud_sync"
            if "note" in path_str or "move" in path_str:
                return "note_and_move", "mobile_hardware"
            if "shortcut" in path_str:
                return "reference_shortcuts", "shortcuts"
            if "third party plugin" in path_str or "vst" in path_str:
                return "third_party_plugins", "plugins"
            if any(k in path_str for k in [
                "creative", "technique", "workflow", "bible", "drum rack secrets",
                "remix", "piano roll", "one view", "electronic music production",
                "mistakes and how to fix", "in a day", "sourcing, sequencing",
                "learn-live", "analog one-click", "automation in ableton"
            ]):
                return "production_guides", "workflow"
            if "knowledge" in path_str or "ableton-live-knowledge-base-guide" in path_str or "faq" in path_str or "extensions-guide" in path_str or "what's new" in path_str or "skins.pdf" in path_str or "release notes" in path_str:
                return "knowledgebase", "support_faq"

        if len(rel.parts) > 1:
            cat = slugify(rel.parts[0])
            subcat = slugify(rel.parts[1]) if len(rel.parts) > 2 else cat
        else:
            ref_dir = self.add_dir if self.add_dir else self.source_dir
            if ref_dir and ref_dir.is_dir():
                cat = slugify(ref_dir.name)
            elif self.name:
                cat = slugify(self.name)
            else:
                cat = "general"
            subcat = cat
        return cat, subcat

    def _extract_jsonl_file(self, path: Path, start_doc_id: int) -> int:
        """Extract structured records from a JSONL file into distinct documents."""
        print(f"[*] Parsing JSONL dataset file: {path.name}")
        doc_id = start_doc_id
        xml_dir = self.dest_dir / "xml_presets"

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line_idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except Exception:
                    continue

                if any(k in data for k in ("xml_preset", "chain_of_thought", "layman_prompt")):
                    doc_id = self._process_ableton_rack_record(data, line_idx, path, doc_id, xml_dir)
                else:
                    doc_id = self._process_generic_jsonl_record(data, line_idx, path, doc_id)

        return doc_id

    def _process_ableton_rack_record(self, data: dict[str, Any], idx: int, path: Path, doc_id: int, xml_dir: Path) -> int:
        xml = data.get("xml_preset", "")
        cot = data.get("chain_of_thought", "")
        layman = data.get("layman_prompt", "")
        technical = data.get("technical_prompt", "")
        emotional = data.get("emotional_prompt", "")

        title = None
        patterns = [
            r"titled [\'\"]([^\'\"]+)[\'\"]",
            r"named [\'\"]([^\'\"]+)[\'\"]",
            r"preset [\'\"]([^\'\"]+)[\'\"]",
            r"Rack: [\'\"]([^\'\"]+)[\'\"]",
            r"Rack named [\'\"]([^\'\"]+)[\'\"]",
            r"Rack titled [\'\"]([^\'\"]+)[\'\"]",
            r"preset is called [\'\"]([^\'\"]+)[\'\"]",
            r"preset known as [\'\"]([^\'\"]+)[\'\"]",
        ]
        for p in patterns:
            m = re.search(p, cot, re.IGNORECASE)
            if m:
                t = m.group(1).strip()
                if t.lower() not in ("wet", "dry", "lo", "mid", "high", "near", "far", "input gain", "cabinets"):
                    title = t
                    break

        if not title:
            ignore_words = {"wet", "dry", "lo", "mid", "high", "near", "far", "input gain", "cabinets", "depth", "hybrid reverb"}
            try:
                root = ET.fromstring(xml)
                for elem in root.iter():
                    if elem.tag == "UserName" and elem.get("Value"):
                        val = elem.get("Value").strip()
                        if val and val.lower() not in ignore_words:
                            title = val
                            break
            except Exception:
                pass

        if not title:
            for text in [technical, layman]:
                for p in patterns:
                    m = re.search(p, text, re.IGNORECASE)
                    if m:
                        t = m.group(1).strip()
                        if t.lower() not in ("wet", "dry", "lo", "mid", "high", "near", "far"):
                            title = t
                            break
                if title:
                    break

        if not title or title.lower() in ("wet", "dry"):
            if idx == 1: title = "3-Way Split Vocoder Rack"
            elif idx == 12: title = "Beat Repeat Multi-LFO Glitch Rack"
            elif idx == 24: title = "Triple LFO EQ Eight Modulator"
            elif idx == 33: title = "Filtered Tempo-Synced Delay Rack"
            elif idx == 68: title = "Dual Parallel Phaser-Flanger Rack"
            elif idx == 88: title = "Full Chain Master"
            elif idx == 89: title = "Master Bus EQ Saturator Glue Limiter Chain"
            else:
                first = layman.split("\n")[0].strip()
                if len(first) < 40 and not first.lower().startswith(("this is", "analyze")):
                    title = first
                else:
                    title = f"Ableton Live Effect Rack {idx+1}"

        title_lower = title.lower()
        cot_lower = cot.lower()
        tech_lower = technical.lower()
        lay_lower = layman.lower()
        comb = f"{title_lower} {cot_lower} {tech_lower} {lay_lower}"

        if any(k in comb for k in ["vocoder", "vocal", "voice"]):
            cat = "vocal_processing"
            subcat = "vocoder"
        elif re.search(r"\b(amp|amps|guitar|cabinet|distort|saturat|overdrive|crunch|fuzz)\b", title_lower) or any(k in comb for k in ["guitar amp", "bass amp", "cabinet", "overdrive", "distortion", "saturator", "fuzz"]):
            cat = "amps_distortion"
            subcat = "guitar_amp" if ("guitar" in comb or "amp" in comb) else "saturation_distortion"
        elif any(k in comb for k in ["reverb", "delay", "echo", "space", "ambience", "room", "hall"]):
            cat = "time_based_spatial"
            subcat = "reverb" if "reverb" in comb else "delay"
        elif any(k in comb for k in ["beat repeat", "glitch", "stutter", "slice"]):
            cat = "rhythm_glitch"
            subcat = "glitch_repeat"
        elif any(k in comb for k in ["chorus", "flanger", "phaser", "tremolo", "modulat", "vibrato", "rotary", "lfo"]):
            cat = "modulation"
            subcat = "lfo_modulation" if "lfo" in comb else "chorus_flanger_phaser"
        elif any(k in comb for k in ["filter", "eq", "equaliz", "sweep", "morph", "spectral"]):
            cat = "filters_eq"
            subcat = "filter" if "filter" in comb else "eq"
        elif any(k in comb for k in ["master", "compress", "gate", "limiter", "dynamic", "sidechain", "transient"]):
            cat = "dynamics_mastering"
            subcat = "mastering" if "master" in comb else "dynamics"
        elif any(k in comb for k in ["synth", "lead", "pad", "instrument"]):
            cat = "instruments_synths"
            subcat = "synth"
        else:
            cat = "creative_fx"
            subcat = "sound_design"

        doc_slug = slugify(title)
        base_uid = f"{cat}_{doc_slug}"
        doc_uid = base_uid
        counter = 2
        existing_uids = {d["doc_uid"] for d in self.docs}
        while doc_uid in existing_uids:
            doc_uid = f"{base_uid}_{counter}"
            counter += 1

        macros = re.findall(r"Macro \d+ \(([^)]+)\)", cot)
        if not macros:
            macros = re.findall(r"Macro \d+:?\s*([A-Za-z0-9\s/]+?)(?:\s*\(|$|\.|\n)", cot)
            macros = [m.strip() for m in macros if m.strip() and len(m.strip()) < 30]

        if xml and len(xml.strip()) > 50:
            xml_dir.mkdir(parents=True, exist_ok=True)
            xml_file = xml_dir / f"{doc_uid}.xml"
            xml_file.write_text(xml, encoding="utf-8")

        md_lines = [
            f"## Sound Character & Overview",
            f"> **Layman Description:** {layman.strip()}" if layman else "",
            "",
            f"> **Emotional Vibe:** {emotional.strip()}" if emotional else "",
            "",
            f"## Technical Architecture & Signal Chain",
            f"{technical.strip()}" if technical else "",
            "",
            f"## Chain of Thought Sound Design Breakdown",
            f"{cot.strip()}" if cot else "",
            "",
        ]
        if macros:
            md_lines.append("### Mapped Macro Controls")
            for m_idx, m_name in enumerate(macros[:8], 1):
                md_lines.append(f"- **Macro {m_idx}:** `{m_name}`")
            md_lines.append("")

        md_lines.extend([
            "## Producer Pal / Live Actions Blueprint",
            "```python",
            f"# Live Actions device chain reference for {title}",
            f'live_create_device(track_index=0, device_name="AudioEffectGroupDevice", params=[',
            f'    {{"name": "Preset Name", "value": "{title}"}},',
        ])
        for m_idx, m_name in enumerate(macros[:4], 1):
            md_lines.append(f'    {{"name": "Macro {m_idx} ({m_name})", "value": 64}},')
        md_lines.extend([
            "])",
            "```",
            "",
            "## XML Source Note",
            f"The full XML preset is cataloged in the dataset database and archived under `xml_presets/{doc_uid}.xml` ({len(xml):,} bytes).",
        ])

        raw_text = "\n".join(l for l in md_lines if l is not None).strip()
        words = len(raw_text.split())
        summary = " ".join((layman or technical or cot).split()[:80])

        doc_entry = {
            "id": doc_id,
            "doc_uid": doc_uid,
            "title": title,
            "category": cat,
            "subcategory": subcat,
            "source_file": path.name,
            "relative_source": f"{path.name}#line_{idx+1}",
            "file_type": "rack_preset",
            "page_count": max(1, words // 350),
            "word_count": words,
            "char_count": len(raw_text),
            "summary": summary,
            "raw_text": raw_text
        }
        self.docs.append(doc_entry)
        self.categories.setdefault(cat, []).append(doc_entry)
        return doc_id + 1

    def _process_generic_jsonl_record(self, data: dict[str, Any], idx: int, path: Path, doc_id: int) -> int:
        title = data.get("title") or data.get("name") or data.get("id") or f"Record {idx+1}"
        cat = slugify(data.get("category") or data.get("type") or path.stem)
        subcat = slugify(data.get("subcategory") or cat)

        lines = [f"# {title}", ""]
        for k, v in data.items():
            if isinstance(v, (str, int, float, bool)):
                lines.append(f"**{k.replace('_', ' ').title()}:** {v}\n")
            elif isinstance(v, (list, dict)):
                lines.append(f"**{k.replace('_', ' ').title()}:**\n```json\n{json.dumps(v, indent=2)}\n```\n")

        raw_text = "\n".join(lines).strip()
        words = len(raw_text.split())
        doc_slug = slugify(str(title))
        base_uid = f"{cat}_{doc_slug}_{idx+1:04d}"
        doc_uid = base_uid

        doc_entry = {
            "id": doc_id,
            "doc_uid": doc_uid,
            "title": str(title),
            "category": cat,
            "subcategory": subcat,
            "source_file": path.name,
            "relative_source": f"{path.name}#line_{idx+1}",
            "file_type": "jsonl_record",
            "page_count": max(1, words // 350),
            "word_count": words,
            "char_count": len(raw_text),
            "summary": " ".join(raw_text.split()[:80]),
            "raw_text": raw_text
        }
        self.docs.append(doc_entry)
        self.categories.setdefault(cat, []).append(doc_entry)
        return doc_id + 1

    def _resolve_section_category(self, title: str, idx: int) -> tuple[str, str]:
        """Categorize section by topic and domain."""
        t_low = title.lower()
        if "infinite_crate" in self.safe_name or "infinite-crate" in self.safe_name or "crate" in self.safe_name:
            if any(k in t_low for k in ["overview", "introduction", "table of contents", "the infinite crate", "lyria realtime", "interface"]):
                return "overview_and_foundations", "overview"
            if any(k in t_low for k in ["workflow", "dig in the crate", "jam along", "prompt dj", "sampling", "jamming"]):
                return "creative_workflows", "production_workflows"
            if any(k in t_low for k in ["install", "setup", "download", "gemini api", "macos", "windows", "platform"]):
                return "installation_and_setup", "system_configuration"
            if any(k in t_low for k in ["control", "parameter", "mixer", "guidance", "sampling diversity", "timbre"]) or re.search(r"\b(stem|stems|bpm)\b", t_low):
                return "plugin_controls", "parameters"
            if any(k in t_low for k in ["troubleshoot", "limitation", "latency", "duration", "dpi", "reset"]):
                return "troubleshooting_and_limits", "troubleshooting"
            if any(k in t_low for k in ["architecture", "developer", "internals", "juce", "react", "c++", "websocket"]):
                return "architecture_and_internals", "developer_internals"
            if any(k in t_low for k in ["community", "resource", "github", "research", "blog", "forum"]):
                return "community_and_resources", "resources"

        if "sonic" in self.safe_name or "sonic_pi" in self.safe_name:
            if re.match(r"^1(\.|\s|$)", title) or "welcome" in t_low or "interface" in t_low or "learning through play" in t_low:
                return "welcome_and_basics", "getting_started"
            if re.match(r"^2(\.|\s|$)", title) or "synth" in t_low or "beep" in t_low or "envelope" in t_low:
                return "synths_and_sound", "synthesis"
            if re.match(r"^3(\.|\s|$)", title) or "sample" in t_low:
                return "samples", "sample_manipulation"
            if re.match(r"^4(\.|\s|$)", title) or "random" in t_low:
                return "programming_and_logic", "randomisation"
            if re.match(r"^5(\.|\s|$)", title) or any(k in t_low for k in ["block", "iteration", "loop", "conditional", "thread", "function", "variable", "synchronisation"]):
                return "programming_and_logic", "structures"
            if re.match(r"^6(\.|\s|$)", title) or "fx" in t_low:
                return "studio_fx_and_control", "studio_fx"
            if re.match(r"^7(\.|\s|$)", title) or "controlling" in t_low or "sliding" in t_low:
                return "studio_fx_and_control", "sound_control"
            if re.match(r"^8(\.|\s|$)", title) or any(k in t_low for k in ["list", "chord", "scale", "ring", "ring chain"]):
                return "data_structures", "musical_structures"
            if re.match(r"^9(\.|\s|$)", title) or "live loop" in t_low or "ticking" in t_low:
                return "live_coding_and_timing", "live_loops"
            if re.match(r"^10(\.|\s|$)", title) or "time state" in t_low or "sync" in t_low or "pattern matching" in t_low:
                return "live_coding_and_timing", "time_state"
            if re.match(r"^11(\.|\s|$)", title) or "midi" in t_low:
                return "midi_osc_and_multichannel", "midi"
            if re.match(r"^12(\.|\s|$)", title) or "osc" in t_low:
                return "midi_osc_and_multichannel", "osc"
            if re.match(r"^13(\.|\s|$)", title) or "multichannel" in t_low or "sound in" in t_low or "live audio" in t_low or "sound out" in t_low:
                return "midi_osc_and_multichannel", "multichannel"
            if "conclusion" in t_low:
                return "reference_and_performance", "conclusion"
            if "magpi" in t_low or (idx >= 60 and idx <= 80) or any(k in t_low for k in ["minecraft", "acid bass", "bizet", "reich", "tips", "sequencer", "amplitude modulation", "classroom"]):
                return "magpi_articles", "magpi_tutorials"
            if any(k in t_low for k in ["shortcut", "screen reader", "sharing", "performing", "knowledge"]):
                return "reference_and_performance", "appendices"

        m = re.match(r"^(\d+)", title)
        if m:
            num = int(m.group(1))
            return f"chapter_{num:02d}", f"section_{idx:02d}"
        return "guide_sections", f"section_{idx:02d}"

    def _extract_multi_section_markdown(self, path: Path, raw_parts: list[str], start_doc_id: int) -> int:
        """Extract multi-chapter guide into individual modular documents (<15k characters)."""
        print(f"[*] Splitting multi-section guide '{path.name}' into modular chapters...")
        doc_id = start_doc_id
        seen_uids: dict[str, int] = {d["doc_uid"]: 1 for d in self.docs}
        seen_filenames: dict[str, int] = {f"{d['category']}/{d.get('filename')}": 1 for d in self.docs}

        for idx, part in enumerate(raw_parts):
            lines = [l.strip() for l in part.strip().splitlines() if l.strip()]
            if not lines:
                continue
            if len(part.strip()) < 100 and idx >= len(raw_parts) - 2 and not any(l.startswith("#") for l in lines):
                continue

            h1 = None
            h2 = None
            for l in lines:
                if l.startswith("# ") and not h1:
                    h1 = l[2:].strip()
                elif l.startswith("## ") and not h2:
                    h2 = l[3:].strip()

            raw_title = h1 or h2 or f"Chapter {idx + 1}"
            raw_title = re.sub(r"^\s*-\s*", "", raw_title)
            if raw_title == "Sonic Pi Tutorial" and len(lines) > 5:
                for l in lines[1:]:
                    if l.startswith("# "):
                        raw_title = l[2:].strip()
                        break
            clean_title = raw_title.replace("Sonic Pi Tutorial", "").strip() or raw_title
            clean_title = clean_title.replace("\ufffd", " - ").strip()
            if "infinite_crate" in self.safe_name or "infinite-crate" in self.safe_name or "crate" in self.safe_name:
                if idx == 0 and "infinite crate" in raw_title.lower():
                    clean_title = "The Infinite Crate: Introduction & Interface"
                elif raw_title.lower() == "overview":
                    clean_title = "The Infinite Crate: Overview & Model Capabilities"
                elif "workflow" in raw_title.lower():
                    clean_title = "Creative Workflows & Usage (Sampling, Jamming, Prompt DJing)"
                elif "control" in raw_title.lower() or "parameter" in raw_title.lower():
                    clean_title = "Plugin Controls & Parameters (Mixer, Timbre, Stems)"
                elif "installation" in raw_title.lower() or "setup" in raw_title.lower():
                    clean_title = "Installation & System Setup (Downloads & API Key)"
                elif "troubleshoot" in raw_title.lower():
                    clean_title = "Troubleshooting & Known Limitations (Latency & High-DPI Fix)"
                elif "architecture" in raw_title.lower() or "internals" in raw_title.lower():
                    clean_title = "Codebase Architecture & Developer Internals (JUCE & React)"
                elif "community" in raw_title.lower() or "resource" in raw_title.lower():
                    clean_title = "Community & Additional Resources (Lyria Research & Source Code)"
            if idx >= 60 and idx <= 80 and not clean_title.lower().startswith("magpi"):
                if clean_title not in ("MagPi Articles",):
                    clean_title = f"MagPi: {clean_title}"

            cat, subcat = self._resolve_section_category(clean_title, idx + 1)
            clean_text = re.sub(r"\!\[(.*?)\]\(images\/(.*?)\)", r"![\1](../../images/\2)", part)
            if clean_text.startswith("---"):
                clean_text = re.sub(r"^---\s*", "", clean_text).strip()

            words = len(clean_text.split())
            pages = max(1, words // 350)
            clean_stem = slugify(clean_title)
            base_uid = f"{cat}_{clean_stem}"
            doc_uid = self._make_unique_uid(base_uid)

            target_filename = f"{clean_stem}.md"
            cat_file_key = f"{cat}/{target_filename}"
            if cat_file_key in seen_filenames:
                target_filename = f"{doc_uid}.md"
            seen_filenames[cat_file_key] = 1

            rel = path.relative_to(self.source_dir) if self.source_dir.is_dir() else Path(path.name)
            doc_entry = {
                "id": doc_id,
                "doc_uid": doc_uid,
                "title": clean_title,
                "category": cat,
                "subcategory": subcat,
                "source_file": rel.as_posix(),
                "relative_source": f"{rel.as_posix()}#section_{idx+1}",
                "file_type": "markdown",
                "filename": target_filename,
                "page_count": pages,
                "word_count": words,
                "char_count": len(clean_text),
                "summary": " ".join(clean_text.split()[:80]),
                "raw_text": clean_text
            }
            self.docs.append(doc_entry)
            self.categories.setdefault(cat, []).append(doc_entry)
            doc_id += 1

        print(f"[+] Extracted {doc_id - start_doc_id} modular chapters from {path.name}.")
        return doc_id

    def _extract_multi_section_markdown_add(self, path: Path, raw_parts: list[str], start_doc_id: int) -> int:
        """Extract multi-chapter guide into individual modular documents for incremental add."""
        print(f"[*] Splitting multi-section guide '{path.name}' into modular chapters for addition...")
        doc_id = start_doc_id
        seen_uids: dict[str, int] = {d["doc_uid"]: 1 for d in self.docs}
        existing_filenames: dict[str, int] = {f"{d['category']}/{d.get('filename')}": 1 for d in self.docs}

        for idx, part in enumerate(raw_parts):
            lines = [l.strip() for l in part.strip().splitlines() if l.strip()]
            if not lines:
                continue
            if len(part.strip()) < 100 and idx >= len(raw_parts) - 2 and not any(l.startswith("#") for l in lines):
                continue

            h1 = None
            h2 = None
            for l in lines:
                if l.startswith("# ") and not h1:
                    h1 = l[2:].strip()
                elif l.startswith("## ") and not h2:
                    h2 = l[3:].strip()

            raw_title = h1 or h2 or f"Chapter {idx + 1}"
            raw_title = re.sub(r"^\s*-\s*", "", raw_title)
            if raw_title == "Sonic Pi Tutorial" and len(lines) > 5:
                for l in lines[1:]:
                    if l.startswith("# "):
                        raw_title = l[2:].strip()
                        break
            clean_title = raw_title.replace("Sonic Pi Tutorial", "").strip() or raw_title
            clean_title = clean_title.replace("\ufffd", " - ").strip()
            if "infinite_crate" in self.safe_name or "infinite-crate" in self.safe_name or "crate" in self.safe_name:
                if idx == 0 and "infinite crate" in raw_title.lower():
                    clean_title = "The Infinite Crate: Introduction & Interface"
                elif raw_title.lower() == "overview":
                    clean_title = "The Infinite Crate: Overview & Model Capabilities"
                elif "workflow" in raw_title.lower():
                    clean_title = "Creative Workflows & Usage (Sampling, Jamming, Prompt DJing)"
                elif "control" in raw_title.lower() or "parameter" in raw_title.lower():
                    clean_title = "Plugin Controls & Parameters (Mixer, Timbre, Stems)"
                elif "installation" in raw_title.lower() or "setup" in raw_title.lower():
                    clean_title = "Installation & System Setup (Downloads & API Key)"
                elif "troubleshoot" in raw_title.lower():
                    clean_title = "Troubleshooting & Known Limitations (Latency & High-DPI Fix)"
                elif "architecture" in raw_title.lower() or "internals" in raw_title.lower():
                    clean_title = "Codebase Architecture & Developer Internals (JUCE & React)"
                elif "community" in raw_title.lower() or "resource" in raw_title.lower():
                    clean_title = "Community & Additional Resources (Lyria Research & Source Code)"
            if idx >= 60 and idx <= 80 and not clean_title.lower().startswith("magpi"):
                if clean_title not in ("MagPi Articles",):
                    clean_title = f"MagPi: {clean_title}"

            cat, subcat = self._resolve_section_category(clean_title, idx + 1)
            clean_text = re.sub(r"\!\[(.*?)\]\(images\/(.*?)\)", r"![\1](../../images/\2)", part)
            if clean_text.startswith("---"):
                clean_text = re.sub(r"^---\s*", "", clean_text).strip()

            words = len(clean_text.split())
            pages = max(1, words // 350)
            clean_stem = slugify(clean_title)
            base_uid = f"{cat}_{clean_stem}"
            doc_uid = self._make_unique_uid(base_uid)

            target_filename = f"{clean_stem}.md"
            cat_file_key = f"{cat}/{target_filename}"
            if cat_file_key in existing_filenames:
                target_filename = f"{doc_uid}.md"
            existing_filenames[cat_file_key] = 1

            rel = path.relative_to(self.add_dir) if self.add_dir else Path(path.name)
            doc_entry = {
                "id": doc_id,
                "doc_uid": doc_uid,
                "title": clean_title,
                "category": cat,
                "subcategory": subcat,
                "source_file": rel.as_posix(),
                "relative_source": f"{rel.as_posix()}#section_{idx+1}",
                "file_type": "markdown",
                "filename": target_filename,
                "page_count": pages,
                "word_count": words,
                "char_count": len(clean_text),
                "summary": " ".join(clean_text.split()[:80]),
                "raw_text": clean_text
            }
            self.docs.append(doc_entry)
            self.new_docs.append(doc_entry)
            self.categories.setdefault(cat, []).append(doc_entry)
            existing_filenames[target_filename] = 1
            doc_id += 1

        print(f"[+] Extracted {doc_id - start_doc_id} modular chapters from {path.name}.")
        return doc_id

    def chunk_all_documents(self):
        """Chunk documents into semantic ~500-word sections with tags."""
        print("[*] Chunking documents into semantic units...")
        chunk_id = 1
        for doc in self.docs:
            text = doc.get("raw_text", "")
            words = text.split()
            step = max(100, self.chunk_words - self.chunk_overlap)

            doc_chunks = []
            for i in range(0, len(words), step):
                chunk_slice = words[i : i + self.chunk_words]
                if not chunk_slice:
                    break
                chunk_text = " ".join(chunk_slice)
                keywords = extract_keywords(chunk_text, top_n=8)
                tags = ", ".join(keywords + [doc["category"]])

                heading_match = re.search(r"^(#{1,4}\s+[^\n]+)", chunk_text, re.MULTILINE)
                section_title = heading_match.group(1).lstrip("# ").strip() if heading_match else f"{doc['title']} (Part {len(doc_chunks) + 1})"

                chunk_entry = {
                    "id": chunk_id,
                    "chunk_uid": f"{doc['doc_uid']}_c{len(doc_chunks):03d}",
                    "doc_id": doc["id"],
                    "doc_title": doc["title"],
                    "category": doc["category"],
                    "subcategory": doc["subcategory"],
                    "section_title": section_title,
                    "page_number": max(1, (i // 350) + 1),
                    "chunk_index": len(doc_chunks),
                    "content": chunk_text,
                    "word_count": len(chunk_slice),
                    "tags": tags
                }
                self.chunks.append(chunk_entry)
                doc_chunks.append(chunk_entry)
                chunk_id += 1

        print(f"[+] Created {len(self.chunks)} semantic chunks.")

    def write_modular_markdown(self):
        """Write clean, modular markdown files under dest/markdown/<category>/."""
        print("[*] Writing modular markdown chapters...")
        md_root = self.dest_dir / "markdown"
        if self.source_dir and self.source_dir.is_dir():
            img_src = self.source_dir / "images"
            if img_src.exists() and img_src.is_dir():
                import shutil
                dest_md_img = md_root / "images"
                if not dest_md_img.exists():
                    shutil.copytree(img_src, dest_md_img)

        for doc in self.docs:
            cat_dir = md_root / doc_rel_dir(doc)
            cat_dir.mkdir(parents=True, exist_ok=True)
            target_file = cat_dir / doc.get("filename", f"{doc['doc_uid']}.md")

            raw = doc.get("raw_text", "").strip()
            if raw.startswith("---") and not raw.startswith("---\nid:") and not raw.startswith('---\n"id":'):
                raw = re.sub(r"^---\s*", "", raw).strip()

            if raw.startswith("---"):
                content = raw
            else:
                frontmatter = (
                    f"---\n"
                    f'id: "{doc["doc_uid"]}"\n'
                    f'title: "{doc["title"]}"\n'
                    f'category: "{doc["category"]}"\n'
                    f'subcategory: "{doc["subcategory"]}"\n'
                    f'pages: {doc["page_count"]}\n'
                    f'words: {doc["word_count"]}\n'
                    f'source: "{doc["source_file"]}"\n'
                    f"---\n\n"
                )
                if not raw.startswith("# "):
                    content = frontmatter + f"# {doc['title']}\n\n" + raw
                else:
                    content = frontmatter + raw
            target_file.write_text(content, encoding="utf-8")

    def build_sqlite_db_preserving_schema(self):
        """Rebuild the SQLite DB for an --add run without changing any
        table's existing schema. Different datasets were built by different
        tool versions and carry different chunks/chunks_fts column sets
        (e.g. Ableton Training's chunks_fts has page_number and lacks
        subcategory/tags, unlike the current canonical shape). A blind
        canonical rebuild silently drops or adds columns that a dataset's
        own custom search.py (AbletonTrainingDB, MusicTheoryDB, etc.)
        depends on. Capture each table's CREATE SQL before dropping,
        recreate verbatim, then insert only the columns each table actually
        has -- schema-aware, same principle as Phase 3/4's `_cols()`."""
        db_path = self.dest_dir / f"{self.safe_name}.db"
        if not db_path.exists():
            self.build_sqlite_db()
            return

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL")
        table_sql = {name: sql for name, sql in cur.fetchall()}
        conn.close()

        db_path.unlink()
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()

        for tbl in ("documents", "chunks", "chunks_fts", "categories"):
            if tbl in table_sql:
                cur.execute(table_sql[tbl])

        def cols_of(table: str) -> list[str]:
            cur.execute(f"PRAGMA table_info({table})")
            return [r[1] for r in cur.fetchall()]

        def value_for(row: dict, col: str):
            if col in row:
                return row[col]
            if col == "tags":
                return row.get("tags") or row.get("category", "")
            if col == "char_count":
                return len(row.get("content") or row.get("raw_text") or "")
            return None

        if "documents" in table_sql:
            doc_cols = cols_of("documents")
            doc_rows = [tuple(value_for(d, c) for c in doc_cols) for d in self.docs]
            cur.executemany(
                f"INSERT INTO documents ({','.join(doc_cols)}) VALUES ({','.join('?' for _ in doc_cols)})",
                doc_rows,
            )

        if "chunks" in table_sql:
            chunk_cols = cols_of("chunks")
            chunk_rows = [tuple(value_for(c, col) for col in chunk_cols) for c in self.chunks]
            cur.executemany(
                f"INSERT INTO chunks ({','.join(chunk_cols)}) VALUES ({','.join('?' for _ in chunk_cols)})",
                chunk_rows,
            )

        if "chunks_fts" in table_sql:
            fts_cols = cols_of("chunks_fts")
            fts_rows = [tuple(value_for(c, col) for col in fts_cols) for c in self.chunks]
            cur.executemany(
                f"INSERT INTO chunks_fts ({','.join(fts_cols)}) VALUES ({','.join('?' for _ in fts_cols)})",
                fts_rows,
            )

        if "categories" in table_sql:
            cat_cols = cols_of("categories")
            cat_rows = []
            for cat_name, c_docs in self.categories.items():
                doc_cnt = len(c_docs)
                cat_chunks = [c for c in self.chunks if c["category"] == cat_name]
                w_cnt = sum(d.get("word_count", 0) for d in c_docs)
                desc = f"Collection of {doc_cnt} documents covering {cat_name.replace('_', ' ')}."
                row_map = {
                    "name": cat_name, "description": desc, "doc_count": doc_cnt,
                    "chunk_count": len(cat_chunks), "word_count": w_cnt,
                }
                cat_rows.append(tuple(row_map.get(c) for c in cat_cols))
            cur.executemany(
                f"INSERT INTO categories ({','.join(cat_cols)}) VALUES ({','.join('?' for _ in cat_cols)})",
                cat_rows,
            )

        conn.commit()
        conn.close()
        print(f"[+] SQLite FTS5 database rebuilt preserving original schema ({db_path.stat().st_size:,} bytes).")

    def build_sqlite_db(self):
        """Build SQLite FTS5 full-text search database."""
        db_path = self.dest_dir / f"{self.safe_name}.db"
        print(f"[*] Building SQLite FTS5 database at {db_path}...")
        if db_path.exists():
            db_path.unlink()

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE documents (
            id INTEGER PRIMARY KEY,
            doc_uid TEXT UNIQUE,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT,
            source_file TEXT,
            file_type TEXT,
            page_count INTEGER,
            word_count INTEGER,
            char_count INTEGER,
            summary TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE chunks (
            id INTEGER PRIMARY KEY,
            chunk_uid TEXT UNIQUE,
            doc_id INTEGER REFERENCES documents(id),
            doc_title TEXT,
            category TEXT,
            subcategory TEXT,
            section_title TEXT,
            page_number INTEGER,
            chunk_index INTEGER,
            content TEXT NOT NULL,
            word_count INTEGER,
            tags TEXT
        )
        """)

        cur.execute("""
        CREATE VIRTUAL TABLE chunks_fts USING fts5(
            chunk_uid UNINDEXED,
            doc_title,
            category,
            subcategory,
            section_title,
            content,
            tags,
            tokenize = 'porter unicode61'
        )
        """)

        cur.execute("""
        CREATE TABLE categories (
            name TEXT PRIMARY KEY,
            description TEXT,
            doc_count INTEGER,
            chunk_count INTEGER,
            word_count INTEGER
        )
        """)

        doc_rows = [
            (
                d["id"], d["doc_uid"], d["title"], d["category"], d["subcategory"],
                d["source_file"], d["file_type"], d["page_count"], d["word_count"],
                d["char_count"], d["summary"]
            )
            for d in self.docs
        ]
        cur.executemany("INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?,?,?)", doc_rows)

        # .get("tags", ...) is deliberate: some datasets were built by an earlier
        # version of this tool (or a related one) whose chunks jsonl rows never
        # carried a "tags" field -- e.g. Ableton Training. A hard c["tags"]
        # lookup would KeyError on every pre-existing chunk the moment --add
        # rebuilds the DB from load_existing() + new chunks.
        chunk_rows = [
            (
                c["id"], c["chunk_uid"], c["doc_id"], c["doc_title"], c["category"],
                c["subcategory"], c["section_title"], c["page_number"], c["chunk_index"],
                c["content"], c["word_count"], c.get("tags") or c.get("category", "")
            )
            for c in self.chunks
        ]
        cur.executemany("INSERT INTO chunks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", chunk_rows)

        fts_rows = [
            (
                c["chunk_uid"], c["doc_title"], c["category"], c["subcategory"],
                c["section_title"], c["content"], c.get("tags") or c.get("category", "")
            )
            for c in self.chunks
        ]
        cur.executemany("INSERT INTO chunks_fts VALUES (?,?,?,?,?,?,?)", fts_rows)

        cat_rows = []
        for cat_name, c_docs in self.categories.items():
            doc_cnt = len(c_docs)
            cat_chunks = [c for c in self.chunks if c["category"] == cat_name]
            w_cnt = sum(d["word_count"] for d in c_docs)
            desc = f"Collection of {doc_cnt} documents covering {cat_name.replace('_', ' ')}."
            cat_rows.append((cat_name, desc, doc_cnt, len(cat_chunks), w_cnt))
        cur.executemany("INSERT INTO categories VALUES (?,?,?,?,?)", cat_rows)

        conn.commit()
        conn.close()
        print(f"[+] SQLite FTS5 database written ({db_path.stat().st_size:,} bytes).")

    def build_parquet_and_jsonl(self):
        """Write Parquet and JSONL dataset formats."""
        chunks_jsonl = self.dest_dir / f"{self.safe_name}_chunks.jsonl"
        docs_jsonl = self.dest_dir / f"{self.safe_name}_docs.jsonl"
        print(f"[*] Writing JSONL files...")
        with open(chunks_jsonl, "w", encoding="utf-8") as f:
            for c in self.chunks:
                f.write(json.dumps(c, ensure_ascii=False) + "\n")
        with open(docs_jsonl, "w", encoding="utf-8") as f:
            for d in self.docs:
                clean_d = {k: v for k, v in d.items() if k != "raw_text"}
                f.write(json.dumps(clean_d, ensure_ascii=False) + "\n")

        if HAS_PYARROW:
            parquet_path = self.dest_dir / f"{self.safe_name}.parquet"
            print(f"[*] Writing Apache Parquet to {parquet_path}...")
            table = pa.Table.from_pylist(self.chunks)
            pq.write_table(table, parquet_path, compression="snappy")
            print(f"[+] Parquet written ({parquet_path.stat().st_size:,} bytes).")

    def generate_search_script(self):
        """Create standalone executable search.py tool."""
        search_path = self.dest_dir / "search.py"
        script_code = f'''#!/usr/bin/env python3
"""Interactive SQLite FTS5 Search Tool for {self.name}."""

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB_PATH = Path(__file__).resolve().parent / "{self.safe_name}.db"

def sanitize_query(q: str) -> str:
    \"\"\"Format user query safely for SQLite FTS5 without syntax errors on dots/hyphens.\"\"\"
    if '"' in q or "MATCH" in q or "NEAR" in q:
        return q
    tokens = re.findall(r"[a-zA-Z0-9_\\u0080-\\uffff]+", q)
    if not tokens:
        return f'\"{{q}}\"'
    return " ".join(f'\"{{t}}\"' for t in tokens)

def search(query: str, limit: int = 5, category: str = None, as_json: bool = False):
    if not DB_PATH.is_file():
        print(f"Error: Database file not found at {{DB_PATH}}", file=sys.stderr)
        return

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    safe_query = sanitize_query(query)
    params = [safe_query]
    cat_filter = ""
    if category:
        cat_filter = "AND category = ?"
        params.append(category)
    params.append(limit)

    sql = f"""
    SELECT
        chunk_uid,
        doc_title,
        category,
        section_title,
        snippet(chunks_fts, 5, '>>> ', ' <<<', '...', 25) as snip,
        bm25(chunks_fts) as rank
    FROM chunks_fts
    WHERE chunks_fts MATCH ? {{cat_filter}}
    ORDER BY rank
    LIMIT ?
    """

    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
    except Exception as exc:
        print(f"Search error: {{exc}}", file=sys.stderr)
        conn.close()
        return

    conn.close()

    if as_json:
        results = [
            {{"chunk_uid": r[0], "doc_title": r[1], "category": r[2], "section_title": r[3], "snippet": r[4], "rank": r[5]}}
            for r in rows
        ]
        print(json.dumps(results, indent=2))
        return

    print(f"\\nFound {{len(rows)}} results for {{query!r}}:\\n")
    for r in rows:
        print(f"  * [{{r[2]}}] {{r[1]}} - {{r[3]}} (Score: {{r[5]:.2f}})")
        print(f"    UID: {{r[0]}}")
        print(f"    Snippet: {{r[4]}}\\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search {self.name} Knowledgebase")
    parser.add_argument("query", help="Search query string")
    parser.add_argument("--limit", type=int, default=5, help="Max results")
    parser.add_argument("--category", help="Filter by category")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args()
    search(args.query, args.limit, args.category, args.json)
'''
        search_path.write_text(script_code, encoding="utf-8")
        print(f"[+] Created search script: {search_path}")

    def generate_index_md(self):
        """Create high-density INDEX.md router table for Producer Pal the APP."""
        index_path = self.dest_dir / "INDEX.md"
        preset = DOMAIN_PRESETS.get(self.safe_name, DOMAIN_PRESETS.get(self.domain, DOMAIN_PRESETS["music"]))

        lines = [
            f"# {self.name} — Producer Index & Device Mapping",
            "",
            f"> **Dataset:** `{self.name}`  ",
            f"> **Coverage:** {len(self.docs)} documents · {sum(d['word_count'] for d in self.docs):,} words across {len(self.categories)} categories  ",
            f"> **Pal Integration:** {preset['scope']}  ",
            "",
            self.description,
            "",
            "---",
            "",
            "## How Producer Pal Reads This Dataset",
            "",
            "This dataset is designed for dual-path discovery:",
            "1. **Live Actions Helper / Genius (Desktop App):**",
            "   - Uses `library_search` to match keywords across this file's routing tables.",
            "   - Opens target markdown files via `library_read(path=\"markdown/...\", query=\"...\")`.",
            "   - Adheres to token budgets: all individual `.md` files are modular (<15,000 characters).",
            "2. **AI Skills (`producer-pal-genius`, `ai-database-browser`):**",
            "   - Inspects `INDEX.md` directly following the **Decide first** protocol.",
            "   - Executes instant BM25 full-text queries via `search.py` against the SQLite FTS5 database.",
            "",
            "---",
            "",
            "## Live Actions / Producer Pal Capability Matrix",
            "",
            "### What Live Actions / Pal CAN Do Natively:",
        ]

        for cap in preset["capabilities"]:
            lines.append(f"- [x] {cap}")
        lines.append("")
        lines.append("### What Remains a Cited Handoff:")
        for lim in preset["limitations"]:
            lines.append(f"- [ ] {lim}")
        lines.append("")
        lines.append("### Action Blueprint Example:")
        lines.append(f"**{preset['example_action']['title']}**")
        lines.append("```python")
        lines.append(preset["example_action"]["code"])
        lines.append("```")
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("## Decide First: Quick Topic Routing Table")
        lines.append("")
        lines.append("Match the user's intent to one of the rows below, then open only the target file:")
        lines.append("")
        lines.append("| Topic / Ask | Target File | Anchor / Section | Live Actions Action |")
        lines.append("| :--- | :--- | :--- | :--- |")

        routing_docs = self.docs if len(self.docs) <= 50 else [docs[0] for docs in self.categories.values()]
        for doc in routing_docs:
            clean_cat = doc["category"].replace("_", " ").title()
            fname = doc.get("filename", f"{doc['doc_uid']}.md")
            rel_path = f"markdown/{doc_rel_dir(doc)}/{fname}"
            title = doc["title"].replace("|", "-")
            lines.append(f"| **{clean_cat}**: {title} | `{rel_path}` | `#{doc['doc_uid']}` | Reference & Set Action |")

        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("## Full Document Catalog by Category")
        lines.append("")

        for cat_name, cat_docs in sorted(self.categories.items()):
            clean_cat = cat_name.replace("_", " ").title()
            lines.append(f"### Category: {clean_cat} ({len(cat_docs)} documents)")
            lines.append("")
            lines.append("| Document Title | Relative Path | Words | Summary / Keywords |")
            lines.append("| :--- | :--- | :--- | :--- |")
            for doc in sorted(cat_docs, key=lambda d: (d.get("subcategory") or "", d["title"])):
                fname = doc.get("filename", f"{doc['doc_uid']}.md")
                rel_path = f"markdown/{doc_rel_dir(doc)}/{fname}"
                title = doc["title"].replace("|", "-")
                words = doc["word_count"]
                summary = doc["summary"][:100].replace("|", "-").replace("\n", " ").strip()
                lines.append(f"| {title} | `{rel_path}` | {words:,} | {summary} |")
            lines.append("")

        index_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"[+] Created INDEX.md: {index_path} ({index_path.stat().st_size:,} bytes)")

    def generate_schema_and_readme(self):
        """Create SCHEMA.md and README.md documentation."""
        schema_path = self.dest_dir / "SCHEMA.md"
        readme_path = self.dest_dir / "README.md"

        schema_text = f"""# {self.name} Knowledge Database Schema

## SQLite Tables (`{self.safe_name}.db`)

### Table: `documents`
Stores metadata for all {len(self.docs)} indexed documents.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | Unique integer document ID |
| `doc_uid` | TEXT UNIQUE | Slugified unique document identifier |
| `title` | TEXT | Document title |
| `category` | TEXT | Primary category classification |
| `subcategory` | TEXT | Secondary subcategory classification |
| `source_file` | TEXT | Original filename |
| `file_type` | TEXT | Format (pdf, markdown, text) |
| `page_count` | INTEGER | Page count |
| `word_count` | INTEGER | Total words |
| `char_count` | INTEGER | Total characters |
| `summary` | TEXT | Opening summary extract |

### Table: `chunks`
Stores all semantic chunks (~500 words).

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | Unique chunk ID |
| `chunk_uid` | TEXT UNIQUE | Unique chunk slug |
| `doc_id` | INTEGER | Foreign key to documents(id) |
| `doc_title` | TEXT | Parent document title |
| `category` | TEXT | Category identifier |
| `section_title` | TEXT | Section heading or page title |
| `content` | TEXT | Clean chunk text |
| `word_count` | INTEGER | Word count |
| `tags` | TEXT | Extracted keyword tags |

### Virtual Table: `chunks_fts` (SQLite FTS5)
Full-Text Search virtual table with Porter stemming and BM25 scoring.
"""
        schema_path.write_text(schema_text, encoding="utf-8")

        readme_text = f"""# {self.name} Dataset

{self.description}

## Quick Stats
- **Total Documents:** {len(self.docs)}
- **Total Chunks:** {len(self.chunks)}
- **Total Words:** {sum(d['word_count'] for d in self.docs):,}
- **Categories:** {len(self.categories)} ({', '.join(sorted(self.categories.keys()))})

## Producer Pal (App) & Skill Compatibility

This dataset is fully formatted for dual-system integration:

### 1. Producer Pal Desktop Host (`Live Actions Helper` / `Genius`)
- **Routing:** Open `INDEX.md` for fast topic routing and Live Actions parameter blueprints.
- **File Access:** Read individual chapters under `markdown/` via `library_read` (all files kept under token limits).
- **Live Control:** Action blocks provide ready-to-call parameter arrays for `live_create_device`, `live_update_device`, and `live_write_notes`.

### 2. AI Skills & CLI (`producer-pal-genius`, `ai-database-browser`)
- **Full-Text BM25 Search:** Run `python search.py "<query>"` for instant SQLite FTS5 search results.
- **SQL Queries:** Direct connection to `{self.safe_name}.db`.
- **Parquet / JSONL:** Data science access via `{self.safe_name}.parquet` and JSONL streams.
"""
        readme_path.write_text(readme_text, encoding="utf-8")
        print(f"[+] Created SCHEMA.md and README.md.")

    def load_existing(self):
        """Load existing documents and chunks from destination dataset."""
        docs_file = self.dest_dir / f"{self.safe_name}_docs.jsonl"
        chunks_file = self.dest_dir / f"{self.safe_name}_chunks.jsonl"

        if not docs_file.exists():
            # Search for any _docs.jsonl in dest_dir
            candidates = list(self.dest_dir.glob("*_docs.jsonl"))
            if candidates:
                docs_file = candidates[0]
                self.safe_name = docs_file.name.replace("_docs.jsonl", "")
                chunks_file = self.dest_dir / f"{self.safe_name}_chunks.jsonl"
            else:
                raise FileNotFoundError(f"Cannot add to dataset: no _docs.jsonl found in {self.dest_dir}")

        print(f"[*] Loading existing dataset from: {self.dest_dir}")
        with open(docs_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    d = json.loads(line)
                    self.docs.append(d)
                    self.categories.setdefault(d["category"], []).append(d)

        if chunks_file.exists():
            with open(chunks_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.chunks.append(json.loads(line))

        print(f"[+] Loaded {len(self.docs)} existing documents and {len(self.chunks)} existing chunks.")

    def scan_and_extract_add(self):
        """Scan addition directory and extract new documents."""
        print(f"[*] Scanning addition source: {self.add_dir}")
        valid_exts = {".pdf", ".md", ".txt", ".json", ".jsonl", ".yaml", ".yml", ".html", ".xml"}
        source_files = [p for p in self.add_dir.glob("**/*") if p.is_file() and p.suffix.lower() in valid_exts]

        clean_files = []
        for p in source_files:
            rel_parts = p.relative_to(self.add_dir).parts
            if any(part.startswith(".") or part == "__pycache__" for part in rel_parts):
                continue
            if p.name.lower().startswith("unins"):
                continue
            if p.name.lower() in ("manifest.json", "package-lock.json", "manifest.jsonl"):
                continue
            # Skip companion rendered PDF if source markdown is co-located
            if p.suffix.lower() == ".pdf" and ((p.parent / "guide.md").exists() or (p.parent / f"{p.stem}.md").exists()):
                continue
            clean_files.append(p)

        print(f"[*] Found {len(clean_files)} candidate files to add.")
        start_doc_id = (max(d["id"] for d in self.docs) + 1) if self.docs else 1
        doc_id = start_doc_id
        seen_uids = {d["doc_uid"]: 1 for d in self.docs}
        existing_filenames = {f"{d['category']}/{d.get('filename')}": 1 for d in self.docs}

        for p in clean_files:
            if p.suffix.lower() == ".jsonl":
                doc_id = self._extract_jsonl_file(p, doc_id)
                continue

            # Support multi-chapter markdown guides (e.g. guide-capture output with --- delimiters or ^# headings)
            if p.suffix.lower() == ".md":
                raw_text, _ = extract_text_from_file(p)
                parts = []
                if "\n---\n" in raw_text or "\n\n---\n\n" in raw_text:
                    parts = [pt.strip() for pt in re.split(r"\n\s*---\s*\n", raw_text) if pt.strip()]
                elif len(re.findall(r"^#\s+[^\n]+", raw_text, re.MULTILINE)) >= 3:
                    parts = [pt.strip() for pt in re.split(r"\n(?=#\s+)", raw_text) if pt.strip()]

                h_count = sum(1 for pt in parts if re.search(r"^#{1,3}\s+", pt, re.MULTILINE))
                if len(parts) >= 3 and h_count >= 3:
                    doc_id = self._extract_multi_section_markdown_add(p, parts, doc_id)
                    continue

            rel = p.relative_to(self.add_dir)
            cat, subcat = self._resolve_category(rel)

            text, pages = extract_text_from_file(p)
            if not text or len(text.strip()) < 50:
                continue

            words = len(text.split())

            raw_stem = p.stem
            raw_stem = re.sub(r"^\d+[\s\-_]+", "", raw_stem)
            raw_stem = raw_stem.replace("·", "-").replace("_", " ").replace("-", " ").strip()
            raw_stem = re.sub(r"\s+", " ", raw_stem)
            clean_title = raw_stem
            clean_title = re.sub(r"\bmidi\b", "MIDI", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bjson\b", "JSON", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bump\b", "UMP", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\basio\b", "ASIO", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\busb\b", "USB", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bdns\b", "DNS", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bsdk\b", "SDK", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\barm64\b", "Arm64", clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r"\bapi\b", "API", clean_title, flags=re.IGNORECASE)

            h1_match = None
            if p.suffix.lower() == ".md":
                h1_match = re.search(r"^#\s+([^\n]+)", text, re.MULTILINE)
                if h1_match and not any(h1_match.group(1).strip().lower().startswith(x) for x in ["source:", "http", "now loading"]):
                    clean_title = h1_match.group(1).strip()

            clean_title = clean_title.replace("\ufffd", " - ").replace("–", "-").replace("—", "-").strip()

            if p.stem.lower() in ("guide", "doc", "manual") and len(rel.parts) > 1:
                clean_stem = slugify(p.parent.name)
            elif p.stem.lower() == "guide" and len(rel.parts) == 1:
                clean_stem = slugify(clean_title) if h1_match else "guide"
            else:
                clean_stem = slugify(re.sub(r"^\d+[\s\-_]+", "", p.stem))

            if clean_stem.startswith(f"{cat}_"):
                base_uid = clean_stem
            else:
                base_uid = f"{cat}_{clean_stem}"

            doc_uid = self._make_unique_uid(base_uid)

            if p.stem.lower() in ("guide", "doc", "manual") and len(rel.parts) > 1:
                target_filename = f"{slugify(p.parent.name)}.md"
            elif p.suffix.lower() == ".md":
                target_filename = f"{clean_stem}.md"
            else:
                target_filename = f"{doc_uid}.md"

            cat_file_key = f"{cat}/{target_filename}"
            if cat_file_key in existing_filenames:
                target_filename = f"{doc_uid}.md"
            existing_filenames[cat_file_key] = 1

            doc_entry = {
                "id": doc_id,
                "doc_uid": doc_uid,
                "title": clean_title,
                "category": cat,
                "subcategory": subcat,
                "source_file": rel.as_posix(),
                "relative_source": rel.as_posix(),
                "file_type": p.suffix.lower().lstrip("."),
                "filename": target_filename,
                "page_count": pages,
                "word_count": words,
                "char_count": len(text),
                "summary": " ".join(text.split()[:80]),
                "raw_text": text
            }
            self.docs.append(doc_entry)
            self.new_docs.append(doc_entry)
            self.categories.setdefault(cat, []).append(doc_entry)
            existing_filenames[target_filename] = 1
            doc_id += 1

        print(f"[+] Successfully extracted {len(self.new_docs)} new documents.")

    def chunk_new_documents(self):
        """Chunk new documents into semantic units and append to self.chunks."""
        print(f"[*] Chunking {len(self.new_docs)} new documents into semantic units...")
        start_chunk_id = (max(c["id"] for c in self.chunks) + 1) if self.chunks else 1
        chunk_id = start_chunk_id

        for doc in self.new_docs:
            text = doc.get("raw_text", "")
            words = text.split()
            step = max(100, self.chunk_words - self.chunk_overlap)

            doc_chunks = []
            for i in range(0, len(words), step):
                chunk_slice = words[i : i + self.chunk_words]
                if not chunk_slice:
                    break
                chunk_text = " ".join(chunk_slice)
                keywords = extract_keywords(chunk_text, top_n=8)
                tags = ", ".join(keywords + [doc["category"]])

                heading_match = re.search(r"^(#{1,4}\s+[^\n]+)", chunk_text, re.MULTILINE)
                section_title = heading_match.group(1).lstrip("# ").strip() if heading_match else f"{doc['title']} (Part {len(doc_chunks) + 1})"

                chunk_entry = {
                    "id": chunk_id,
                    "chunk_uid": f"{doc['doc_uid']}_c{len(doc_chunks):03d}",
                    "doc_id": doc["id"],
                    "doc_title": doc["title"],
                    "category": doc["category"],
                    "subcategory": doc["subcategory"],
                    "section_title": section_title,
                    "page_number": max(1, (i // 350) + 1),
                    "chunk_index": len(doc_chunks),
                    "content": chunk_text,
                    "word_count": len(chunk_slice),
                    "tags": tags
                }
                self.chunks.append(chunk_entry)
                doc_chunks.append(chunk_entry)
                chunk_id += 1

        print(f"[+] Created {len(self.chunks) - (start_chunk_id - 1)} new semantic chunks (Total: {len(self.chunks)}).")

    def write_new_modular_markdown(self):
        """Write clean, modular markdown files for new docs under dest/markdown/<category>/."""
        print(f"[*] Writing {len(self.new_docs)} new modular markdown chapters...")
        md_root = self.dest_dir / "markdown"
        for doc in self.new_docs:
            cat_dir = md_root / doc_rel_dir(doc)
            cat_dir.mkdir(parents=True, exist_ok=True)
            target_file = cat_dir / doc.get("filename", f"{doc['doc_uid']}.md")

            raw = doc.get("raw_text", "").strip()
            frontmatter = (
                f"---\n"
                f'id: "{doc["doc_uid"]}"\n'
                f'title: "{doc["title"]}"\n'
                f'category: "{doc["category"]}"\n'
                f'subcategory: "{doc["subcategory"]}"\n'
                f'pages: {doc["page_count"]}\n'
                f'words: {doc["word_count"]}\n'
                f'source: "{doc["source_file"]}"\n'
                f"---\n\n"
            )
            if not raw.startswith("# "):
                content = frontmatter + f"# {doc['title']}\n\n" + raw
            else:
                content = frontmatter + raw
            target_file.write_text(content, encoding="utf-8")

    def run(self):
        """Execute full build workflow."""
        print(f"\n=======================================================")
        print(f" Building Dataset: {self.name}")
        print(f" Source:      {self.source_dir}")
        print(f" Destination: {self.dest_dir}")
        print(f"=======================================================\n")
        self.validate()
        self.scan_and_extract()
        self.chunk_all_documents()
        self.write_modular_markdown()
        self.build_sqlite_db()
        self.build_parquet_and_jsonl()
        self.generate_search_script()
        self.generate_index_md()
        self.generate_schema_and_readme()
        print(f"\n[SUCCESS] Dataset '{self.name}' successfully built at {self.dest_dir}!\n")

    def update_index_md_incremental(self):
        """Append new_docs into an existing INDEX.md without regenerating it.

        A full regenerate (generate_index_md) would blow away hand-curated
        prose -- Capability Matrix edits, Decide-First routing curation, any
        free text a human added -- that never round-trips through self.docs.
        Instead: bump the coverage line, append new rows into each existing
        '### Category: ...' table (or append a whole new category section at
        the end of the catalog for a genuinely new category), and touch
        nothing else. Falls back to a full generate only if INDEX.md is
        missing entirely (nothing to preserve)."""
        index_path = self.dest_dir / "INDEX.md"
        if not index_path.exists():
            self.generate_index_md()
            return

        text = index_path.read_text(encoding="utf-8")

        total_docs = len(self.docs)
        total_words = sum(d["word_count"] for d in self.docs)
        total_cats = len(self.categories)
        text = re.sub(
            r"> \*\*Coverage:\*\* [\d,]+ documents · [\d,]+ words across \d+ categories",
            f"> **Coverage:** {total_docs:,} documents · {total_words:,} words across {total_cats} categories",
            text,
            count=1,
        )

        new_by_cat: dict[str, list[dict]] = {}
        for doc in self.new_docs:
            new_by_cat.setdefault(doc["category"], []).append(doc)

        def row_for(doc: dict) -> str:
            fname = doc.get("filename", f"{doc['doc_uid']}.md")
            rel_path = f"markdown/{doc_rel_dir(doc)}/{fname}"
            title = doc["title"].replace("|", "-")
            words = doc["word_count"]
            summary = doc["summary"][:100].replace("|", "-").replace("\n", " ").strip()
            return f"| {title} | `{rel_path}` | {words:,} | {summary} |"

        for cat_name, docs_to_add in new_by_cat.items():
            clean_cat = cat_name.replace("_", " ").title()
            heading_re = re.compile(
                rf"^### Category: {re.escape(clean_cat)} \((\d+) documents?\)\s*$", re.MULTILINE
            )
            m = heading_re.search(text)
            if m:
                old_count = int(m.group(1))
                new_count = old_count + len(docs_to_add)
                text = text[: m.start()] + f"### Category: {clean_cat} ({new_count} documents)" + text[m.end() :]

                # Find the end of this category's table: scan forward from the
                # heading to the next blank-line-then-("###"|"##"|EOF) boundary,
                # and insert new rows as the last rows of that table.
                search_from = m.start()
                next_heading = re.search(r"^#{2,3}\s", text[search_from + len(clean_cat) :], re.MULTILINE)
                section_end = (
                    search_from + len(clean_cat) + next_heading.start()
                    if next_heading
                    else len(text)
                )
                section = text[search_from:section_end]
                # last non-blank line of the section is the last table row
                lines = section.rstrip("\n").split("\n")
                insert_at = len(lines)
                new_rows = [row_for(d) for d in docs_to_add]
                lines[insert_at:insert_at] = new_rows
                patched_section = "\n".join(lines) + "\n\n"
                text = text[:search_from] + patched_section + text[section_end:]
            else:
                # Brand new category: append a whole new section at the end
                new_section = [f"### Category: {clean_cat} ({len(docs_to_add)} documents)", ""]
                new_section.append("| Document Title | Relative Path | Words | Summary / Keywords |")
                new_section.append("| :--- | :--- | :--- | :--- |")
                new_section.extend(row_for(d) for d in docs_to_add)
                new_section.append("")
                text = text.rstrip("\n") + "\n\n" + "\n".join(new_section) + "\n"

        index_path.write_text(text, encoding="utf-8")
        print(f"[+] Updated INDEX.md incrementally ({len(self.new_docs)} new rows, existing content preserved)")

    def run_add(self):
        """Execute incremental addition workflow."""
        print(f"\n=======================================================")
        print(f" Adding to Dataset: {self.name}")
        print(f" Addition Source: {self.add_dir}")
        print(f" Destination:     {self.dest_dir}")
        print(f"=======================================================\n")
        if not self.dest_dir.exists():
            raise FileNotFoundError(f"Destination directory does not exist: {self.dest_dir}")
        if not self.add_dir or not self.add_dir.exists():
            raise FileNotFoundError(f"Addition source directory does not exist: {self.add_dir}")

        self.load_existing()
        self.scan_and_extract_add()
        if not self.new_docs:
            print("[!] No new documents found to add.")
            return
        self.chunk_new_documents()
        self.write_new_modular_markdown()
        self.build_sqlite_db_preserving_schema()
        self.build_parquet_and_jsonl()
        if (self.dest_dir / "search.py").exists():
            print("[*] search.py already exists -- leaving it in place (schema is additive; "
                  "custom wrapper classes some datasets carry must not be overwritten).")
        else:
            self.generate_search_script()
        self.update_index_md_incremental()
        self.generate_schema_and_readme()
        print(f"\n[SUCCESS] Successfully added {len(self.new_docs)} documents to '{self.name}' at {self.dest_dir}!\n")


# ── CLI Interface ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="dataset-builder: Compile documents into a dual-access Producer Pal App + AI Skill dataset."
    )
    parser.add_argument("--source", help="Source directory containing raw documents/PDFs (for full build)")
    parser.add_argument("--add", help="Addition source directory containing documents/PDFs to append into an existing dataset")
    parser.add_argument("--dest", required=True, help="Destination directory (MANDATORY: must be specified first)")
    parser.add_argument("--name", help="Dataset name (defaults to destination folder name)")
    parser.add_argument("--description", help="Short description of dataset")
    parser.add_argument("--domain", default="music", choices=["music", "general"], help="Domain preset for Live Actions capability matrix")
    parser.add_argument("--chunk-words", type=int, default=500, help="Target words per chunk")
    parser.add_argument("--chunk-overlap", type=int, default=50, help="Overlap words between chunks")

    args = parser.parse_args()

    if not args.source and not args.add:
        parser.error("Either --source (for full build) or --add (for incremental addition) must be specified.")

    builder = DatasetBuilder(
        source_dir=Path(args.source) if args.source else None,
        dest_dir=Path(args.dest),
        name=args.name,
        description=args.description,
        domain=args.domain,
        chunk_words=args.chunk_words,
        chunk_overlap=args.chunk_overlap,
        add_dir=Path(args.add) if args.add else None,
    )

    if args.add:
        builder.run_add()
    else:
        builder.run()


if __name__ == "__main__":
    main()
