# discogs-mcp

A Model Context Protocol (MCP) server for the [Discogs API](https://www.discogs.com/developers/).

## Tools

| Tool | Description |
|---|---|
| `search_discogs` | Search releases, masters, artists, and labels |
| `get_release` | Full details for a release by ID |
| `get_master_release` | Master release details (canonical entry for a title) |
| `get_master_versions` | All known pressings/versions of a master |
| `get_artist` | Artist profile by ID |
| `get_artist_releases` | List an artist's releases |
| `get_label` | Record label details by ID |
| `get_label_releases` | List a label's releases |
| `get_marketplace_stats` | Current lowest price, median price, # for sale |
| `get_price_suggestions` | Suggested price by condition (Mint, NM, VG+, etc.) |

## Setup

### 1. Install Node.js

Download and install from https://nodejs.org (v20 LTS recommended).

### 2. Get a Discogs Personal Access Token

1. Log in at https://www.discogs.com
2. Go to **Settings → Developers**
3. Click **Generate new token**
4. Copy the token

### 3. Install dependencies and build

```bash
cd discogs-mcp
npm install
npm run build
```

### 4. Add to Claude Desktop

Edit your Claude Desktop config file:
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "discogs": {
      "command": "node",
      "args": ["C:\\Users\\KJ-NoJesteringStudio\\Claude\\discogs-mcp\\dist\\index.js"],
      "env": {
        "DISCOGS_TOKEN": "your_token_here"
      }
    }
  }
}
```

Restart Claude Desktop and the Discogs tools will appear.

## Example usage

Once connected, you can ask Claude things like:

- *"Search Discogs for Pink Floyd's The Wall"*
- *"Get marketplace stats for release 1954579 in USD"*
- *"What are the price suggestions for release 249504?"*
- *"List all versions of master release 5427"*
- *"Show me releases by artist 45467"*
