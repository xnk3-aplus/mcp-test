# MCP Demo Server

A Python-based Model Context Protocol (MCP) server demonstrating tool functionality with beautiful HTML UI using FastMCP.

## Features

This MCP server includes an interactive dice rolling tool with a professional UI:

- **`roll_dice(sides)`** - Roll a dice with a specified number of sides (default: 6) and get a beautifully formatted HTML page with:
  - Large dice emoji display (for 6-sided dice)
  - Result highlighted in a clean, modern interface
  - Detailed information about the roll
  - FastMCP-styled UI components

## Prerequisites

- Python 3.8+
- pip
- [ngrok](https://ngrok.com/) (for public internet access)

## Installation

1. Clone this repository:
```bash
git clone https://github.com/xnk3-aplus/mcp-test.git
cd mcp-test
```

2. Install required dependencies:
```bash
pip install fastmcp
```

## Running the Server

### Local Development

To run the server locally on port 8000:

```bash
python server.py
```

The server will start at `http://localhost:8000`

### Making Your Server Publicly Accessible

Your server must be accessible from the internet for external integrations. For development, use ngrok:

**Terminal 1** - Start the MCP server:
```bash
python server.py
```

**Terminal 2** - Expose via ngrok:
```bash
ngrok http 8000
```

**Important:** Note your public URL (e.g., `https://abc123.ngrok.io`) for connecting to the server.

## Using the Server

Once your server is running, you can call the `roll_dice` tool through any MCP-compatible client:

```python
# Example: Roll a standard 6-sided dice
roll_dice()

# Example: Roll a 20-sided dice
roll_dice(sides=20)
```

## API Endpoint

The MCP server exposes its interface at:
```
POST /mcp/
```

## Tech Stack

- **FastMCP** - Fast MCP server implementation
- **Python 3** - Runtime environment

## Project Structure

```
mcp-test/
├── server.py          # Main MCP server with dice rolling tool
└── README.md          # This file
```

## License

MIT License - Feel free to use and modify as needed.