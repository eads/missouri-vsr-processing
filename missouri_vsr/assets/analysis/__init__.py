"""Analysis asset collections, organized by the post they are published in.

Each post is one module (e.g. ``initial_impressions_2025``) whose assets share a
``group_name`` and emit publish-ready artifacts under ``data/out/analysis/<post>/``.
These read from the processed pipeline (canonical_combined) and validate every
statewide figure against the official statewide report (state_reports_combined).
"""
