#!/bin/bash
# Installation script for cursor-analytics-mcp

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Installing Cursor Analytics MCP Server...${NC}"

# Check if required components exist (now included locally)
if  [ ! -d ".cursor" ]; then
    echo -e "${RED}Error: Required cursor-analytics components not found${NC}"
    echo "Please ensure utils/, context/, and .cursor/ directories are present"
    exit 1
fi

echo -e "${GREEN}✅ Found required cursor-analytics components${NC}"

# Install uv if not already available
if ! command -v uv &> /dev/null; then
    echo -e "${GREEN}Installing uv package manager...${NC}"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

# Create virtual environment with uv (requires Python 3.10+)
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment with uv (Python 3.10+)...${NC}"
    uv venv venv --python 3.10 || uv venv venv --python 3.11 || uv venv venv --python 3.12
fi

# Activate virtual environment
echo -e "${GREEN}Activating virtual environment...${NC}"
source venv/bin/activate

# Install the MCP server and all dependencies with uv
echo -e "${GREEN}Installing cursor-analytics-mcp with FastMCP and all dependencies...${NC}"
uv pip install -e .

# Create config directory if it doesn't exist
mkdir -p config
echo -e "${YELLOW}Config directory created at config/. Add your .env file here if needed.${NC}"

echo -e "${GREEN}Installation complete!${NC}"
echo
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Ensure your Snowflake credentials are configured:"
echo "   export SNOWFLAKE_USER='your.username'"
echo "   export SNOWFLAKE_PASSWORD='your_password'"
echo
echo "2. Test the MCP server:"
echo "   source venv/bin/activate"
echo "   cursor-analytics-mcp"
echo
echo "3. Add to your MCP client configuration:"
echo "   See config.json.example for sample configuration"
echo
echo -e "${GREEN}🎉 Cursor Analytics MCP Server is ready to use!${NC}"
echo -e "${YELLOW}Note: The server uses the original MCP protocol for Python 3.9 compatibility${NC}"