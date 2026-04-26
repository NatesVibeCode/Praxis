from __future__ import annotations

from surfaces.api.handlers.workflow_query_core import _extract_data_dictionary_table, _has_data_dictionary_intent

def test_extract_data_dictionary_table_preserves_schema() -> None:
    assert _extract_data_dictionary_table("what is the schema of public.workflow_runs") == "public.workflow_runs"
    assert _extract_data_dictionary_table("show columns for 'auth.users'") == "auth.users"
    assert _extract_data_dictionary_table("describe table \"custom_schema.my_table\"?") == "custom_schema.my_table"

def test_has_data_dictionary_intent_guards_generic_names() -> None:
    # Generic question about table names should NOT be interpreted as a lookup for a table named "names"
    # Even if _extract_data_dictionary_table might pick up "names" from the end of the sentence.
    assert _has_data_dictionary_intent("which table names are available") == True
    
    # But a specific table named "names" should still work if asked clearly
    assert _extract_data_dictionary_table("describe table names") == "names"
