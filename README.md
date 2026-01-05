# MCP UI Server Demo

A Python-based Model Context Protocol (MCP) server demonstrating UI resource capabilities using `mcp-ui-server`. This server provides multiple interactive tools that return rich HTML interfaces.

## Features

This MCP server includes four demonstration tools:

- **`greet()`** - Simple greeting with styled HTML
- **`show_dashboard()`** - Sample dashboard displaying server metrics
- **`show_external_site()`** - Embeds external websites via iframe
- **`show_interactive_demo()`** - Interactive UI with JavaScript intent messaging

## Prerequisites

- Python 3.8+
- pip
- [ngrok](https://ngrok.com/) (for public internet access)

## Installation

1. Clone this repository:
```bash
git clone <your-repo-url>
cd mcp-test
```

2. Install required dependencies:
```bash
pip install fastmcp mcp-ui-server
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

**Important:** Note your public URL (e.g., `https://abc123.ngrok.io`) for the next steps.

## Using the Tools

Once your server is running, you can call the available tools through any MCP-compatible client:

- **greet** - Returns a simple welcome message
- **show_dashboard** - Displays server metrics in a grid layout
- **show_external_site** - Shows example.com in an iframe
- **show_interactive_demo** - Includes clickable buttons that send intents

## API Endpoint

The MCP server exposes its interface at:
```
POST /mcp/
```

## Tech Stack

- **FastMCP** - Fast MCP server implementation
- **mcp-ui-server** - UI resource creation utilities
- **Python 3** - Runtime environment

## Project Structure

```
mcp-test/
├── server.py          # Main MCP server with UI tools
└── README.md          # This file
```

## License

MIT License - Feel free to use and modify as needed.