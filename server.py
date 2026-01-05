from fastmcp import FastMCP
from fastmcp.utilities.ui import (
    create_page,
    create_logo,
    create_status_message,
    create_info_box,
    create_detail_box
)
import random

mcp = FastMCP("Demo Server")

@mcp.tool
def roll_dice(sides: int = 6) -> str:
    """Roll a dice with the specified number of sides and display the result in a beautiful UI."""
    result = random.randint(1, sides)
    
    # Create the dice emoji based on result (for standard 6-sided dice)
    dice_emoji = {
        1: "⚀", 2: "⚁", 3: "⚂", 
        4: "⚃", 5: "⚄", 6: "⚅"
    }
    dice_display = dice_emoji.get(result, "🎲") if sides == 6 else "🎲"
    
    # Create page content
    logo = create_logo()
    
    # Big dice result display
    dice_result = f"""
        <div style="text-align: center; margin: 40px 0;">
            <div style="font-size: 120px; margin: 20px 0;">{dice_display}</div>
            <div style="font-size: 48px; font-weight: bold; color: #2563eb; margin: 20px 0;">
                {result}
            </div>
        </div>
    """
    
    # Detail information
    details = create_detail_box([
        ("Dice Type", f"{sides}-sided dice"),
        ("Result", str(result)),
        ("Range", f"1 - {sides}")
    ])
    
    # Success message
    success_msg = create_status_message(
        f"Successfully rolled a {sides}-sided dice!",
        is_success=True
    )
    
    # Combine all content
    content = f"""
        {logo}
        <h1 style="text-align: center; color: #1e293b; margin-top: 30px;">
            🎲 Dice Roll Result
        </h1>
        {success_msg}
        {dice_result}
        {details}
        <div style="text-align: center; margin-top: 30px; color: #64748b; font-size: 14px;">
            Roll again to get a new result!
        </div>
    """
    
    # Create full page
    html_page = create_page(
        content=content,
        title="Dice Roll - FastMCP Demo",
        additional_styles="""
            body {
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
            }
        """
    )
    
    return html_page

if __name__ == "__main__":
    mcp.run(transport="http", port=8000)