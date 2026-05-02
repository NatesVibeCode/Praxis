"""Materialize-side authority for the unified materialize + Review front door.

Foundation slice: typed payload contracts for the chat-model Review surface
that both lanes (``auto`` — "Materialize it for me", and ``manifest`` —
"Build the Manifest") consume.

The :mod:`runtime.materialize` package houses typed contracts for the new
lanes. Naming was migrated from ``runtime.compile`` because "compile" collides
with the standard tech term (source → bytecode); the verb in Praxis is
"materialize" — turning intent into graph rows.
"""
