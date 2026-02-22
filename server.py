#!/usr/bin/env python3
"""
openeo-mcp: MCP (Model Context Protocol) server for OpenEO and STAC APIs.

Provides AI assistants with tools to:
  - Browse and query OpenEO-compatible backends (collections, processes)
  - Submit and monitor OpenEO batch jobs
  - Search and retrieve STAC (SpatioTemporal Asset Catalog) items

Default backend: Copernicus Data Space Ecosystem (CDSE)
  - OpenEO: https://openeo.dataspace.copernicus.eu
  - STAC:   https://catalogue.dataspace.copernicus.eu/stac

Configuration (environment variables):
  OPENEO_BACKEND_URL  OpenEO backend endpoint (default: CDSE)
  STAC_API_URL        STAC API endpoint (default: CDSE STAC)
  OPENEO_TOKEN        Optional pre-existing OIDC access token

Authentication:
  Anonymous access is sufficient for read-only operations (collections, STAC search).
  Batch job submission requires authentication — use the `openeo_authenticate` tool
  to start an OIDC Device Flow login with your CDSE account.

Usage:
  python3 server.py                         # stdio mode (for MCP clients)
  OPENEO_TOKEN=xxx python3 server.py        # pre-authenticated

Author: Matteo Mattiuzzi <matteo@mattiuzzi.com>
License: EUPL-1.2
"""

import os
import json
import asyncio
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types


# ─── Configuration ─────────────────────────────────────────────────────────────

OPENEO_BACKEND_URL = os.getenv(
    "OPENEO_BACKEND_URL",
    "https://openeo.dataspace.copernicus.eu"
)
"""OpenEO backend URL. Override to use any OpenEO-compliant backend."""

STAC_API_URL = os.getenv(
    "STAC_API_URL",
    "https://catalogue.dataspace.copernicus.eu/stac"
)
"""STAC API root URL. Must implement STAC API spec (OGC)."""

OPENEO_TOKEN = os.getenv("OPENEO_TOKEN", "")
"""Optional pre-existing OIDC access token. If set, used on every connection."""


# ─── MCP Server instance ───────────────────────────────────────────────────────

server = Server("openeo-mcp")


# ─── Session state ─────────────────────────────────────────────────────────────

_session_connection = None
"""
In-memory authenticated openeo.Connection.
Set by `openeo_authenticate` via OIDC Device Flow.
Reused by all subsequent tool calls that require authentication.
Reset on server restart (stdio session end).
"""


# ─── Connection helpers ────────────────────────────────────────────────────────

def get_openeo_connection():
    """
    Return an openeo.Connection to the configured backend.

    Priority order:
      1. Existing authenticated session (_session_connection) from openeo_authenticate
      2. Token from OPENEO_TOKEN environment variable
      3. Anonymous (unauthenticated) connection — read-only

    Returns:
        openeo.Connection: Ready-to-use connection object.
    """
    global _session_connection
    import openeo

    # Reuse existing authenticated session (from openeo_authenticate tool)
    if _session_connection is not None:
        return _session_connection

    conn = openeo.connect(OPENEO_BACKEND_URL)

    # Authenticate with pre-existing token if provided
    if OPENEO_TOKEN:
        conn.authenticate_oidc_access_token(OPENEO_TOKEN)

    return conn


def get_stac_client():
    """
    Return a pystac_client.Client connected to the configured STAC API.

    Returns:
        pystac_client.Client: Ready-to-use STAC client.
    """
    from pystac_client import Client
    return Client.open(STAC_API_URL)


# ─── Tool definitions ──────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    """
    Register all MCP tools exposed by this server.

    Tools are grouped into two categories:
      - openeo_*: Tools for interacting with OpenEO backends
      - stac_*:   Tools for querying STAC APIs

    Returns:
        List of mcp.types.Tool definitions with name, description, and JSON schema.
    """
    return [
        # ── Authentication ──────────────────────────────────────────────────────
        types.Tool(
            name="openeo_authenticate",
            description=(
                "Authenticate with your CDSE (Copernicus Data Space) account using "
                "OIDC Device Flow. Returns a browser URL — open it to log in. "
                "Once authenticated, all job-related tools (execute, status, download) "
                "will use your personal credentials. No credentials are stored on disk."
            ),
            inputSchema={"type": "object", "properties": {}}
        ),

        # ── OpenEO: Discovery ───────────────────────────────────────────────────
        types.Tool(
            name="openeo_connect",
            description=(
                "Test connectivity to an OpenEO backend and return its capabilities "
                "(API version, title, available endpoint count). Works without authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "backend_url": {
                        "type": "string",
                        "description": (
                            f"OpenEO backend root URL. Defaults to {OPENEO_BACKEND_URL}. "
                            "Other options: https://openeo.terrascope.be, https://openeo.cloud"
                        )
                    }
                }
            }
        ),
        types.Tool(
            name="openeo_list_collections",
            description=(
                "List EO data collections available on the OpenEO backend "
                "(e.g. Sentinel-2, Landsat, MODIS). "
                "Optionally filter by collection ID or title substring. "
                "Works without authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "string",
                        "description": (
                            "Case-insensitive substring filter applied to collection ID and title. "
                            "Example: 'sentinel' returns all Sentinel collections."
                        )
                    }
                }
            }
        ),
        types.Tool(
            name="openeo_describe_collection",
            description=(
                "Retrieve full metadata for a specific OpenEO collection: "
                "spatial/temporal extent, bands, links, and provider info. "
                "Works without authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "collection_id": {
                        "type": "string",
                        "description": (
                            "Exact collection ID as returned by openeo_list_collections. "
                            "Example: 'SENTINEL2_L2A'"
                        )
                    }
                },
                "required": ["collection_id"]
            }
        ),
        types.Tool(
            name="openeo_list_processes",
            description=(
                "List processing functions available on the OpenEO backend "
                "(e.g. NDVI, load_collection, save_result). "
                "Optionally filter by process ID or summary. "
                "Works without authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "string",
                        "description": (
                            "Case-insensitive substring filter on process ID or summary. "
                            "Example: 'ndvi' returns vegetation index processes."
                        )
                    }
                }
            }
        ),

        # ── OpenEO: Job management ──────────────────────────────────────────────
        types.Tool(
            name="openeo_execute_job",
            description=(
                "Submit an OpenEO batch processing job. "
                "Requires prior authentication via openeo_authenticate or OPENEO_TOKEN env var. "
                "The process_graph must follow the OpenEO process graph specification."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "process_graph": {
                        "type": "object",
                        "description": (
                            "OpenEO process graph as a JSON object. "
                            "Must include at least load_collection and save_result nodes. "
                            "See: https://openeo.org/documentation/1.0/processes.html"
                        )
                    },
                    "title": {
                        "type": "string",
                        "description": "Human-readable job title shown in the OpenEO dashboard."
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional longer description of what the job computes."
                    }
                },
                "required": ["process_graph"]
            }
        ),
        types.Tool(
            name="openeo_job_status",
            description=(
                "Check the current status of an OpenEO batch job. "
                "Possible statuses: queued, running, finished, error, canceled. "
                "Requires authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Job ID as returned by openeo_execute_job."
                    }
                },
                "required": ["job_id"]
            }
        ),
        types.Tool(
            name="openeo_download_result",
            description=(
                "Download all result files from a finished OpenEO batch job "
                "to a local directory. Job must have status 'finished'. "
                "Requires authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Job ID of a finished job."
                    },
                    "output_dir": {
                        "type": "string",
                        "description": (
                            "Absolute path to local directory where results will be saved. "
                            "Created automatically if it does not exist."
                        )
                    }
                },
                "required": ["job_id", "output_dir"]
            }
        ),

        # ── STAC ────────────────────────────────────────────────────────────────
        types.Tool(
            name="stac_list_collections",
            description=(
                "List all EO data collections in the STAC API catalog. "
                "Optionally filter by collection ID or title. "
                "No authentication required."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "string",
                        "description": "Case-insensitive substring filter on ID or title."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of collections to return. Default: 50."
                    }
                }
            }
        ),
        types.Tool(
            name="stac_search",
            description=(
                "Search for EO data items in the STAC catalog using spatial and/or temporal filters. "
                "Returns item IDs, acquisition dates, bounding boxes, and available asset types. "
                "No authentication required."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "collections": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "List of collection IDs to search within. "
                            "Example: ['SENTINEL-2']"
                        )
                    },
                    "bbox": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": (
                            "Bounding box as [west, south, east, north] in WGS84 (EPSG:4326). "
                            "Example: [11.0, 46.0, 13.0, 48.0] for South Tyrol."
                        )
                    },
                    "datetime": {
                        "type": "string",
                        "description": (
                            "Date or date range in ISO 8601 format. "
                            "Single date: '2024-06-01'. Range: '2024-01-01/2024-06-30'."
                        )
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of items to return. Default: 10."
                    },
                    "query": {
                        "type": "object",
                        "description": (
                            "Additional property filters (STAC API query extension). "
                            "Example: {'eo:cloud_cover': {'lt': 20}} for <20% cloud cover."
                        )
                    }
                }
            }
        ),
        types.Tool(
            name="stac_get_collection",
            description=(
                "Retrieve full metadata for a specific STAC collection: "
                "spatial/temporal extent, license, and links. "
                "No authentication required."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "collection_id": {
                        "type": "string",
                        "description": "STAC collection ID as returned by stac_list_collections."
                    }
                },
                "required": ["collection_id"]
            }
        ),
        types.Tool(
            name="stac_get_item",
            description=(
                "Retrieve a specific STAC item (scene/granule) with full metadata "
                "including all asset download links. "
                "No authentication required."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "collection_id": {
                        "type": "string",
                        "description": "STAC collection ID the item belongs to."
                    },
                    "item_id": {
                        "type": "string",
                        "description": "Unique STAC item ID (scene identifier)."
                    }
                },
                "required": ["collection_id", "item_id"]
            }
        ),
    ]


# ─── Tool dispatcher ───────────────────────────────────────────────────────────

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """
    MCP entry point for tool calls. Dispatches to _dispatch() and handles errors.

    Args:
        name:      Tool name (must match a registered tool in list_tools).
        arguments: Tool arguments as parsed from the MCP request.

    Returns:
        List with a single TextContent containing JSON-serialized result or error message.
    """
    try:
        result = await _dispatch(name, arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    except Exception as e:
        # Return structured error — don't crash the server
        return [types.TextContent(type="text", text=f"Error: {type(e).__name__}: {e}")]


async def _dispatch(name: str, args: dict) -> Any:
    """
    Route tool calls to their implementation.

    Args:
        name: Tool name.
        args: Validated arguments dict.

    Returns:
        Python dict/list to be JSON-serialized by call_tool.

    Raises:
        ValueError: If tool name is unknown.
        openeo.OpenEoClientException: On OpenEO API errors.
        requests.HTTPError: On STAC HTTP errors.
    """

    # ── Authentication ──────────────────────────────────────────────────────────

    if name == "openeo_authenticate":
        """
        Start OIDC Device Flow for CDSE.
        Stores the authenticated connection in _session_connection for reuse.
        """
        global _session_connection
        import openeo

        conn = openeo.connect(OPENEO_BACKEND_URL)

        # Device code flow: user visits a URL printed to stdout and logs in.
        # max_poll_time controls how long we wait for the user (seconds).
        conn.authenticate_oidc_device(
            client_id="openeo-platform",
            max_poll_time=300,  # 5 minute window for user to authenticate
        )

        # Cache the authenticated connection for subsequent tool calls
        _session_connection = conn

        user_info = conn.describe_account()
        return {
            "status": "authenticated",
            "user": user_info.get("user_id", "unknown"),
            "backend": OPENEO_BACKEND_URL,
            "message": "You are now logged in. Job tools will use your CDSE account.",
        }

    # ── OpenEO: Discovery ───────────────────────────────────────────────────────

    elif name == "openeo_connect":
        """Connect anonymously and return backend capabilities summary."""
        url = args.get("backend_url", OPENEO_BACKEND_URL)
        import openeo

        conn = openeo.connect(url)
        caps = conn.capabilities()
        return {
            "backend_url": url,
            "api_version": caps.api_version(),
            "backend_version": caps.get("backend_version", "unknown"),
            "title": caps.get("title", ""),
            "description": caps.get("description", ""),
            "endpoints": len(caps.get("endpoints", [])),
        }

    elif name == "openeo_list_collections":
        """List collections, optionally filtered by ID/title substring."""
        conn = get_openeo_connection()
        collections = conn.list_collections()

        f = args.get("filter", "").lower()
        result = []
        for c in collections:
            cid = c.get("id", "")
            title = c.get("title", "")
            # Apply optional text filter
            if not f or f in cid.lower() or f in title.lower():
                result.append({
                    "id": cid,
                    "title": title,
                    "description": c.get("description", "")[:200],  # truncate for readability
                })
        return {"count": len(result), "collections": result}

    elif name == "openeo_describe_collection":
        """Return full collection metadata dict as provided by the backend."""
        conn = get_openeo_connection()
        c = conn.describe_collection(args["collection_id"])
        return dict(c)

    elif name == "openeo_list_processes":
        """List processes, optionally filtered by ID/summary substring."""
        conn = get_openeo_connection()
        processes = conn.list_processes()

        f = args.get("filter", "").lower()
        result = []
        for p in processes:
            pid = p.get("id", "")
            summary = p.get("summary", "")
            if not f or f in pid.lower() or f in summary.lower():
                result.append({"id": pid, "summary": summary})
        return {"count": len(result), "processes": result}

    # ── OpenEO: Job management ──────────────────────────────────────────────────

    elif name == "openeo_execute_job":
        """
        Create and immediately start a batch job.
        Returns job_id for use with openeo_job_status and openeo_download_result.
        """
        conn = get_openeo_connection()

        job = conn.create_job(
            process_graph=args["process_graph"],
            title=args.get("title", "openeo-mcp job"),
            description=args.get("description", ""),
        )
        job.start_job()  # Queue the job for execution

        return {
            "job_id": job.job_id,
            "status": job.status(),
            "title": args.get("title"),
        }

    elif name == "openeo_job_status":
        """Return current status and progress of a batch job."""
        conn = get_openeo_connection()
        job = conn.job(args["job_id"])
        info = job.describe_job()

        return {
            "job_id": args["job_id"],
            "status": info.get("status"),      # queued | running | finished | error
            "progress": info.get("progress"),  # 0-100 percentage if available
            "created": info.get("created"),
            "updated": info.get("updated"),
            "error": info.get("error"),        # error message if status == error
        }

    elif name == "openeo_download_result":
        """
        Download all result files of a finished job to a local directory.
        Creates output_dir if it doesn't exist.
        """
        conn = get_openeo_connection()
        job = conn.job(args["job_id"])
        output_dir = args["output_dir"]

        os.makedirs(output_dir, exist_ok=True)
        results = job.get_results()
        results.download_files(output_dir)  # Downloads all assets in parallel

        files = os.listdir(output_dir)
        return {
            "job_id": args["job_id"],
            "output_dir": output_dir,
            "files": files,
        }

    # ── STAC ────────────────────────────────────────────────────────────────────

    elif name == "stac_list_collections":
        """Iterate STAC collections with optional text filter and limit."""
        client = get_stac_client()
        limit = args.get("limit", 50)
        f = args.get("filter", "").lower()

        collections = []
        for c in client.get_collections():
            if not f or f in c.id.lower() or f in (c.title or "").lower():
                collections.append({
                    "id": c.id,
                    "title": c.title,
                    "description": (c.description or "")[:200],
                })
            if len(collections) >= limit:
                break  # Stop once limit reached

        return {"count": len(collections), "collections": collections}

    elif name == "stac_search":
        """
        Search STAC items with spatial/temporal/property filters.
        Only a subset of item properties is returned for readability.
        """
        client = get_stac_client()

        # Build search parameters from provided args (skip missing ones)
        search_params = {}
        if "collections" in args:
            search_params["collections"] = args["collections"]
        if "bbox" in args:
            search_params["bbox"] = args["bbox"]         # [west, south, east, north]
        if "datetime" in args:
            search_params["datetime"] = args["datetime"] # ISO 8601 date or range
        if "query" in args:
            search_params["query"] = args["query"]       # property filters

        limit = args.get("limit", 10)
        search = client.search(**search_params, max_items=limit)

        # Extract relevant fields from each item
        items = []
        for item in search.items():
            items.append({
                "id": item.id,
                "collection": item.collection_id,
                "datetime": str(item.datetime),
                "bbox": item.bbox,
                "assets": list(item.assets.keys()),  # available file types (B02, B03, SCL, etc.)
                # Only include commonly useful properties
                "properties": {k: v for k, v in item.properties.items()
                               if k in ["datetime", "platform", "instrument",
                                        "eo:cloud_cover", "s2:mgrs_tile",
                                        "processing:level"]},
            })
        return {"count": len(items), "items": items}

    elif name == "stac_get_collection":
        """Return collection metadata including extent and license."""
        client = get_stac_client()
        c = client.get_collection(args["collection_id"])

        return {
            "id": c.id,
            "title": c.title,
            "description": c.description,
            "extent": c.extent.to_dict() if c.extent else None,  # spatial + temporal extent
            "license": c.license,
            "links": [{"rel": l.rel, "href": l.href} for l in c.links[:10]],
        }

    elif name == "stac_get_item":
        """
        Fetch a specific STAC item directly via REST API.
        Returns full item GeoJSON including all asset download links.
        """
        import requests

        url = f"{STAC_API_URL}/collections/{args['collection_id']}/items/{args['item_id']}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()  # Raise on 4xx/5xx
        return resp.json()

    else:
        raise ValueError(f"Unknown tool: {name!r}. Check list_tools() for available tools.")


# ─── Entrypoint ────────────────────────────────────────────────────────────────

async def main():
    """
    Start the MCP server in stdio mode.
    MCP clients (Claude Desktop, OpenClaw, etc.) communicate via stdin/stdout.
    The server runs until the client closes the connection.
    """
    async with stdio_server() as streams:
        await server.run(
            streams[0],  # stdin  — incoming MCP messages
            streams[1],  # stdout — outgoing MCP responses
            server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
