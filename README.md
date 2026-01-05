# OKR Analysis MCP Server 🚀

A powerful Model Context Protocol (MCP) server for analyzing OKR (Objectives and Key Results) data. Built with `fastmcp`, this server allows AI assistants like ChatGPT to access, analyze, and report on your organization's OKR progress.

## ✨ Features

- **📉 Monthly OKR Shift Analysis**: Calculates movement of OKRs compared to the end of the previous month.
- **🌳 Visual Tree**: Generates hierarchical OKR trees with clear visualization.
- **📝 Form Data Extraction**: Automatically extracts custom fields (e.g., Priority, Contribution) from Targets.
- **🤖 AI Ready**: Designed for integration with ChatGPT via MCP.

## 🛠️ Prerequisites

- Python 3.10+
- [ngrok](https://ngrok.com/) (For manual deployment)
- Base.vn API Tokens (`GOAL_ACCESS_TOKEN`, `ACCOUNT_ACCESS_TOKEN`)

## 📦 Local Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/xnk3-aplus/mcp-test.git
   cd mcp-test
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment:**
   Create a `.env` file in the root directory:
   ```env
   GOAL_ACCESS_TOKEN=your_goal_token_here
   ACCOUNT_ACCESS_TOKEN=your_account_token_here
   ```

## 🚀 Deployment Options

You can deploy this server in two ways:
1. **☁️ FastMCP Cloud** (Recommended for easiest hosting)
2. **🔌 ChatGPT via Ngrok** (Manual local tunnel)

---

### Option 1: FastMCP Cloud ☁️
*The fastest way to deploy your server securely.*

1. **Push to GitHub**: Ensure your code is in a GitHub repository.
2. **Go to [fastmcp.cloud](https://fastmcp.cloud)** and log in with GitHub.
3. **Create Project**:
   - Select your repository.
   - Entrypoint: `server.py:mcp`
   - **Environment Variables**: Add your `GOAL_ACCESS_TOKEN` and `ACCOUNT_ACCESS_TOKEN` in the dashboard.
4. **Deploy**: Click Deploy. You get a secure URL (e.g., `https://okr-server.fastmcp.app/mcp`).
5. **Connect**: Use this URL in your MCP client (e.g., ChatGPT, Cursor).

---

### Option 2: ChatGPT via Ngrok 🔌
*For local development or direct connection to ChatGPT.*

#### 1. Start Local Server
```bash
python server.py
```
*Runs on `http://localhost:8000`*

#### 2. Expose via Ngrok
```bash
ngrok http 8000
```
*Copy your public https URL (e.g., `https://abc-123.ngrok-free.app`).*

#### 3. Connect to ChatGPT
1. **Enable Developer Mode**:
   - Go to **ChatGPT Settings** → **Connectors** → **Advanced** → Enable **Developer Mode**.
2. **Create Connector**:
   - Click **Create** (top right).
   - **Name**: `OKR Analysis Server`
   - **Server URL**: Paste your ngrok URL (`https://abc-123.ngrok-free.app/mcp/`).
   - Click **Create** and **Trust**.
3. **Use in Chat**:
   - New Chat → Click **+** → **More** → **Developer Mode**.
   - Toggle **OKR Analysis Server** ON.

---

## 📝 API Reference

| Tool | Description | Auto-Run |
|------|-------------|----------|
| `get_monthly_okr_shifts` | Calculates current vs last month values for all users. | ✅ Yes |
| `get_full_okr_data` | Returns the detailed dataset (like CSV) as JSON list. Now includes Form Fields! 📝 | ✅ Yes |
| `get_okr_tree` | Returns a hierarchical visualization of the OKR tree (ASCII/Visual Nodes). 🌳 | ✅ Yes |

## 📄 License
MIT License