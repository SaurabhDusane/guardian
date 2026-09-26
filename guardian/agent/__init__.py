"""Diagnosis agent: evidence bundles, LLM-backed root-cause diagnosis, and its evaluation.

The agent is advisory. It reads Guardian's stores and writes only under
``<root>/diagnoses/``; it never edits code, merges, or promotes.
"""
