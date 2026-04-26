from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add scripts to path so we can import praxis_atlas
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import praxis_atlas

def test_atlas_script_generates_html_with_banner(tmp_path, monkeypatch) -> None:
    # Mock atlas_graph read model
    mock_graph = {"nodes": [], "edges": []}
    mock_read_model = MagicMock()
    mock_read_model.build_graph.return_value = mock_graph
    mock_read_model.AREA_COLORS = {}
    
    monkeypatch.setattr(praxis_atlas, "atlas_graph_read_model", mock_read_model)
    monkeypatch.setattr(praxis_atlas, "REPO_ROOT", tmp_path)
    
    praxis_atlas.main()
    
    out_path = tmp_path / "artifacts" / "atlas.html"
    assert out_path.exists()
    
    html = out_path.read_text()
    assert "OFFLINE EXPORT" in html
    assert "snapshot generated at" in html
    # Check for timestamp pattern (roughly)
    import re
    assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC", html)
