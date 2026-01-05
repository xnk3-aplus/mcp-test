# mcp-test

## Making Your Server Publicly Accessible

Your server must be accessible from the internet. For development, use ngrok:

**Terminal 1**
```bash
python server.py
```

**Terminal 2**
```bash
ngrok http 8000
```

Note your public URL (e.g., `https://abc123.ngrok.io`) for the next steps.