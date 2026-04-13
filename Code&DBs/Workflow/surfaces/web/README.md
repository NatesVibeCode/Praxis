# Praxis Engine Web Pages

Legacy static dashboard assets retained on disk for reference only.
The FastAPI surface no longer serves `/ui`; old `/ui` links now redirect to `/app`.

## Files Created

### 1. `nav.html` (2KB)
Shared navigation component included by other pages via JavaScript fetch.
- **Links**: Dashboard, Queue, Pipeline, Receipts, Leaderboard, Fitness, Costs
- **Features**:
  - Auto-activates current page link with `.active` class
  - Dark theme with cyan (#00d4ff) accents
  - Monospace font (Courier New)
  - Responsive design for mobile

### 2. `leaderboard.html` (18KB)
Model performance ranking table with real-time data from `/api/leaderboard` and `/api/trust`.

**Features**:
- **Sortable columns**: Click any column header to sort
  - Rank, Model, Provider, Workflow Runs, Pass %, ELO Score, Avg Cost ($), Avg Latency (ms), Status
- **Pass rate color coding**:
  - Green (#2d5a2d): ≥90%
  - Yellow (#5a5a2d): 70-89%
  - Red (#5a2d2d): <70%
- **Search**: Filter by model name or provider
- **Statistics cards**: Total models, avg pass rate, top ELO, total workflow runs
- **Auto-refresh**: Every 30 seconds via interval polling
- **Manual refresh**: Button to reload data on demand
- **Responsive table**: Horizontal scroll on mobile

**Data Fetches**:
- `GET /api/leaderboard` — array of models with metrics
- `GET /api/trust` — trust/ELO scores (optional, merged by provider/model)

**Expected API Response Format**:
```json
[
  {
    "model": "gpt-4-turbo",
    "provider": "openai",
    "dispatches": 150,
    "pass_rate": 0.95,
    "elo_score": 1450,
    "avg_cost": 0.0125,
    "avg_latency": 2500,
    "status": "healthy"
  }
]
```

The legacy JSON field name `dispatches` is retained for compatibility even though the UI now labels it as workflow runs.

### 3. `fitness.html` (20KB)
Model × Capability heatmap matrix with quality dimension profiles.

**Features**:
- **Heatmap grid**: Models (rows) × Capabilities (columns)
  - Capabilities: mechanical_edit, code_generation, code_review, architecture, analysis, creative, research, debug
  - Color gradient: Red (low fitness) → Yellow (medium) → Green (high fitness)
  - Fitness scale: Red 0-20%, Orange 20-40%, Yellow 40-60%, Light Green 60-80%, Dark Green 80-100%
  - No Data: Gray
  - Click any cell to view that model's dimension profile
  
- **Dimension Profile Sidebar**: Shows 7 quality dimensions for selected model
  - Dimensions: outcome, correctness, safety, resilience, integration, scope, diligence
  - Horizontal bar chart with color gradient
  - Displayed as percentage with label

- **Search**: Filter models by name
- **Legend**: Fitness score color mapping
- **Auto-refresh**: Every 30 seconds
- **Responsive**: Adapts to tablet/mobile (heatmap scrolls, sidebar moves below)

**Data Fetches**:
- `GET /api/fitness` — capability fitness scores and quality dimensions
- `GET /api/leaderboard` — model list (for building rows)

**Expected API Response Format**:
```json
[
  {
    "run_id": "run:...",
    "provider_slug": "openai",
    "model_slug": "gpt-4-turbo",
    "output_quality_signals": {
      "mechanical_edit": 0.92,
      "code_generation": 0.88,
      "code_review": 0.95,
      "architecture": 0.85,
      "analysis": 0.91,
      "creative": 0.78,
      "research": 0.89,
      "debug": 0.93
    },
    "dimension_scores": {
      "outcome": 0.9,
      "correctness": 0.88,
      "safety": 0.95,
      "resilience": 0.85,
      "integration": 0.87,
      "scope": 0.92,
      "diligence": 0.89
    },
    "recorded_at": "2026-04-03T21:20:53.977661+00:00"
  }
]
```

## Styling

All pages use:
- **Dark theme**: #0a0a0a background, #e0e0e0 text
- **Accent color**: Cyan (#00d4ff)
- **Monospace font**: Courier New
- **Borders**: 1px solid #333
- **Responsive design**: Works on desktop, tablet, mobile
- **No build tools**: Pure HTML + CSS + Vanilla JS (ES6+)

## API Contract

Both pages expect:
- API endpoint at `/api/` (relative)
- JSON responses
- CORS headers to allow cross-origin requests (if hosted separately)

Error handling:
- Network errors display in red banner
- Missing/empty data shows "No data" message
- Failed API calls disable refresh button and show error

## Features

✓ No build tools required — pure vanilla HTML/CSS/JS  
✓ Vanilla JS with modern syntax (fetch, arrow functions, template literals)  
✓ Sortable columns on leaderboard (click header to toggle asc/desc)  
✓ Color-coded pass rates and fitness scores  
✓ Real-time data refresh with auto-polling  
✓ Search/filter functionality  
✓ Responsive design for all screen sizes  
✓ Shared navigation component  
✓ Dark theme matching platform aesthetic  
✓ No external dependencies (no jQuery, no Bootstrap, no frameworks)  

## Usage

1. Host all HTML files in the same directory
2. Ensure API endpoints are accessible at `/api/`
3. Open in web browser (desktop or mobile)
4. Navigation links work when all pages are present

## Notes

- Navigation component uses JavaScript fetch to load `nav.html` into each page
- Current page link auto-activates with `.active` class (cyan underline)
- All data is fetched client-side — pages work with any backend REST API
- Fitness heatmap shows "No data" cells in gray for missing capability scores
- Dimension profile only appears when a model cell is clicked in the fitness matrix
