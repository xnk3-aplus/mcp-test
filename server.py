from fastmcp import FastMCP
import random

mcp = FastMCP("Demo Server")

@mcp.tool
def roll_dice(sides: int = 6) -> int:
    """Roll a dice with the specified number of sides."""
    return random.randint(1, sides)

if __name__ == "__main__":
    mcp.run(transport="http", port=8000)