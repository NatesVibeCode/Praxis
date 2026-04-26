from __future__ import annotations

from surfaces.mcp.tools import discover

def test_tool_praxis_discover_surfaces_context_warning(monkeypatch) -> None:
    # Mock indexer search results
    class FakeIndexer:
        def search(self, **kwargs):
            return [{"name": "test", "module_path": "test.py"}]
            
    # Mock subsystems
    class FakeSubs:
        def get_module_indexer(self):
            return FakeIndexer()
        def get_pg_conn(self):
            return None
            
    monkeypatch.setattr(discover, "_subs", FakeSubs())
    
    # Mock context attachment to fail
    def _raise_error(*args, **kwargs):
        raise RuntimeError("db offline")
        
    monkeypatch.setattr(discover, "attach_interpretive_context_to_items", _raise_error)
    
    params = {"action": "search", "query": "test"}
    result = discover.tool_praxis_discover(params)
    
    assert result["ok"] is True
    assert "warning" in result
    assert "interpretive-context attachment failed: RuntimeError: db offline" in result["warning"]
    assert len(result["results"]) == 1
    assert result["results"][0]["name"] == "test"
