import argparse
from mcp.server.fastmcp import FastMCP
from mcp_ui_server import create_ui_resource
from mcp_ui_server.core import UIResource

# Create FastMCP instance
mcp = FastMCP("my-server")

@mcp.tool()
def show_dashboard() -> list[UIResource]:
    """Display an analytics dashboard."""
    ui_resource = create_ui_resource({
        "uri": "ui://dashboard/analytics",
        "content": {
            "type": "externalUrl",
            "iframeUrl": "https://my.analytics.com/dashboard"
        },
        "encoding": "text"
    })
    return [ui_resource]

@mcp.tool()
def show_welcome() -> list[UIResource]:
    """Display a welcome message."""
    ui_resource = create_ui_resource({
        "uri": "ui://welcome/main",
        "content": {
            "type": "rawHtml",
            "htmlString": "<h1>Welcome to My MCP Server!</h1><p>How can I help you today?</p>"
        },
        "encoding": "text"
    })
    return [ui_resource]

if __name__ == "__main__":
    mcp.run()
