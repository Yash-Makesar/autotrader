#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn] ${NC} $*"; }
error()   { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

echo "========================================"
echo "  Qwen3 Gold Paper Bot — Setup"
echo "========================================"

# ── 1. Python ──────────────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    error "python3 not found. Install Python 3.11+ and re-run."
fi

PY_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)

if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 11 ]; }; then
    error "Python 3.11+ required (found $PY_VERSION)."
fi
info "Python $PY_VERSION found."

# ── 2. Ollama ──────────────────────────────────────────────────────────────────
if ! command -v ollama &>/dev/null; then
    warn "Ollama not found. Installing via official script..."
    curl -fsSL https://ollama.com/install.sh | sh
    info "Ollama installed."
else
    info "Ollama $(ollama --version 2>/dev/null | head -1) found."
fi

# ── 3. Pull qwen3:8b ───────────────────────────────────────────────────────────
if ollama list 2>/dev/null | grep -q "qwen3:8b"; then
    info "qwen3:8b already present."
else
    info "Pulling qwen3:8b (~5 GB, this may take a while)..."
    ollama pull qwen3:8b
    info "qwen3:8b downloaded."
fi

# ── 4. Virtual environment ─────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
    info "Creating virtual environment..."
    python3 -m venv .venv
fi
info "Activating virtual environment."
# shellcheck source=/dev/null
source .venv/bin/activate

# ── 5. Dependencies ────────────────────────────────────────────────────────────
info "Installing Python dependencies..."
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
info "Dependencies installed."

# ── 6. .env file ───────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
    info "Creating .env from template..."
    cat > .env <<'EOF'
# Required — get a free key at https://twelvedata.com
TWELVE_DATA_API_KEY=

# Optional — Telegram trade alerts
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Unused by default
GEMINI_API_KEY=
NEWS_API_KEY=
EOF
    warn ".env created. Open it and set TWELVE_DATA_API_KEY before running the bot."
else
    info ".env already exists — skipping."
fi

# ── 7. Check API key ───────────────────────────────────────────────────────────
if grep -q "^TWELVE_DATA_API_KEY=$" .env 2>/dev/null; then
    warn "TWELVE_DATA_API_KEY is empty in .env!"
    warn "Edit .env and add your key from https://twelvedata.com (free, 800 req/day)."
fi

# ── Done ───────────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "  Setup complete!"
echo "========================================"
echo ""
echo "  Next steps:"
echo "    1. Edit .env  →  set TWELVE_DATA_API_KEY"
echo "    2. source .venv/bin/activate"
echo "    3. python start.py"
echo ""
echo "  Dashboard will be at http://localhost:5001"
echo ""
