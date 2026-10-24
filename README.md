# Dataset Builder Engine

> **Tri-Platform Agent Skill** for Claude Desktop / Code, Grok CLI, and Google Antigravity / Gemini.  
> Dual-access database builder for Live Actions Helper, Ableton AI skills, and documentation corpora.

---

## Overview

`dataset-builder` provides standardized capabilities across conversational AI platforms:
- Turns collections of PDFs, manuals, and docs into dual-access searchable datasets.\n- SQLite FTS5 full-text indexing paired with Parquet and JSONL export formats.\n- Built-in search routing table generation (INDEX.md) and search.py helper script.\n- Enforces strict destination specification and coverage audits.\n- Tri-platform NTFS junctions connecting Claude, Grok, and Antigravity.

---

## Tri-Platform Architecture

Zero duplicate code across platforms. Canonical source resides in `.agents`:
- **Canonical Source**: `C:\Users\<you>\.agents\skills\dataset-builder`
- **Claude**: `C:\Users\<you>\.claude\skills\dataset-builder` (NTFS Junction)
- **Grok**: `C:\Users\<you>\.grok\skills\dataset-builder` (NTFS Junction)
- **Antigravity**: `C:\Users\<you>\.gemini\config\skills\dataset-builder` (NTFS Junction)

---

## Documentation Suite

- [`HOW_TO_USE.txt`](HOW_TO_USE.txt): Standardized user guide and prompt templates.
- [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md): Feature checklist and roadmap.
- [`App Changes Tracker.html`](App%20Changes%20Tracker.html): Interactive visual version log.
- [`SKILL.md`](SKILL.md): Agent system instructions and triggers.
