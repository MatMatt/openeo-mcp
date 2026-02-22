# openeo-mcp

MCP (Model Context Protocol) server for **OpenEO** and **STAC** APIs.
Default backend: [Copernicus Data Space Ecosystem (CDSE)](https://dataspace.copernicus.eu).

## Tools

### OpenEO
| Tool | Description |
|------|-------------|
| `openeo_connect` | Test connection, get backend capabilities |
| `openeo_list_collections` | List collections (with optional filter) |
| `openeo_describe_collection` | Full metadata for a collection |
| `openeo_list_processes` | List available processes |
| `openeo_execute_job` | Submit a batch job (requires auth) |
| `openeo_job_status` | Check job status |
| `openeo_download_result` | Download job results |

### STAC
| Tool | Description |
|------|-------------|
| `stac_list_collections` | List all STAC collections |
| `stac_search` | Search items by bbox/datetime/collection |
| `stac_get_collection` | Get collection metadata |
| `stac_get_item` | Get a specific item |

## Install

```bash
pip install -e .
```

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```bash
OPENEO_BACKEND_URL=https://openeo.dataspace.copernicus.eu
STAC_API_URL=https://catalogue.dataspace.copernicus.eu/stac
OPENEO_TOKEN=   # optional, for authenticated jobs
```

## Usage

```bash
# Run server (stdio mode for MCP)
python server.py

# Or with env file
source .env && python server.py
```

## Claude Desktop Config

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "openeo-mcp": {
      "command": "python",
      "args": ["/path/to/openeo-mcp/server.py"],
      "env": {
        "OPENEO_BACKEND_URL": "https://openeo.dataspace.copernicus.eu",
        "STAC_API_URL": "https://catalogue.dataspace.copernicus.eu/stac",
        "OPENEO_TOKEN": ""
      }
    }
  }
}
```

## OpenClaw / mcporter Config

```json
{
  "name": "openeo-mcp",
  "command": "python /path/to/openeo-mcp/server.py"
}
```

## Authentication (for batch jobs)

```bash
# Get CDSE token via device auth
pip install openeo
python -c "import openeo; openeo.connect('https://openeo.dataspace.copernicus.eu').authenticate_oidc()"
```

## Author

Matteo Mattiuzzi — EEA / CLMS
