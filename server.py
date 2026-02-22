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
import time
import hashlib
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

# ─── Metadata cache (in-memory, TTL-based) ─────────────────────────────────────

CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))  # default: 5 minutes
"""
Cache TTL in seconds for metadata responses (collections, processes).
Set to 0 to disable caching. Override via CACHE_TTL_SECONDS env var.
"""

_cache: dict[str, tuple[Any, float]] = {}
"""Simple dict cache: key → (value, expiry_timestamp)"""


def _cache_get(key: str) -> Any | None:
    """Return cached value if not expired, else None."""
    if key in _cache:
        value, expiry = _cache[key]
        if time.time() < expiry:
            return value
        del _cache[key]
    return None


def _cache_set(key: str, value: Any) -> None:
    """Store value in cache with TTL expiry."""
    if CACHE_TTL > 0:
        _cache[key] = (value, time.time() + CACHE_TTL)


def _cache_key(*parts: str) -> str:
    """Generate a stable cache key from multiple string parts."""
    return hashlib.md5("|".join(parts).encode()).hexdigest()


# ─── Webhook registry ──────────────────────────────────────────────────────────

_webhooks: dict[str, str] = {}
"""
job_id → webhook_url mapping.
When a job is submitted with a webhook URL, the server polls the job
and POSTs the status to the URL when the job finishes or errors.
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

        # ── OpenEO: Synchronous execution ───────────────────────────────────────
        types.Tool(
            name="openeo_execute_sync",
            description=(
                "Execute a small OpenEO process graph synchronously and return the result immediately. "
                "Suitable for lightweight computations (small AOI, short time range). "
                "For large computations use openeo_execute_job (batch). "
                "Requires authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "process_graph": {
                        "type": "object",
                        "description": "OpenEO process graph as JSON object."
                    },
                    "output_format": {
                        "type": "string",
                        "description": "Output format: GTiff, netCDF, JSON. Default: GTiff."
                    },
                    "output_file": {
                        "type": "string",
                        "description": "Local path to save result. If omitted, returns metadata only."
                    }
                },
                "required": ["process_graph"]
            }
        ),

        # ── OpenEO: UDF ─────────────────────────────────────────────────────────
        types.Tool(
            name="openeo_run_udf",
            description=(
                "Run a User-Defined Function (UDF) in Python or R within an OpenEO process graph. "
                "UDFs allow custom pixel-level or timeseries processing beyond built-in processes. "
                "Requires authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "udf_code": {
                        "type": "string",
                        "description": (
                            "UDF source code as a string. "
                            "Python UDFs must define a function with signature: "
                            "apply_datacube(cube: DataCube, context: dict) -> DataCube"
                        )
                    },
                    "udf_language": {
                        "type": "string",
                        "description": "UDF language: 'Python' or 'R'. Default: Python.",
                        "enum": ["Python", "R"]
                    },
                    "collection_id": {
                        "type": "string",
                        "description": "Input collection to apply UDF to."
                    },
                    "bbox": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Bounding box [west, south, east, north] for the UDF run."
                    },
                    "temporal_extent": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Time range as [start, end] in ISO 8601."
                    },
                    "title": {
                        "type": "string",
                        "description": "Job title."
                    }
                },
                "required": ["udf_code", "collection_id", "bbox", "temporal_extent"]
            }
        ),

        # ── OpenEO: Webhook ─────────────────────────────────────────────────────
        types.Tool(
            name="openeo_set_job_webhook",
            description=(
                "Register a webhook URL for a batch job. "
                "The server will POST job status updates (finished/error) to the URL "
                "when the job completes. Polls every 60 seconds in the background."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Job ID to monitor."
                    },
                    "webhook_url": {
                        "type": "string",
                        "description": "HTTPS URL to POST status updates to."
                    }
                },
                "required": ["job_id", "webhook_url"]
            }
        ),

        # ── Cache control ───────────────────────────────────────────────────────
        types.Tool(
            name="cache_clear",
            description=(
                "Clear the in-memory metadata cache. "
                "Useful when backend data has changed and you need fresh results."
            ),
            inputSchema={"type": "object", "properties": {}}
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
        """List collections, optionally filtered by ID/title substring. Results are cached."""
        conn = get_openeo_connection()

        # Check cache first (TTL controlled by CACHE_TTL_SECONDS env var)
        cache_key = _cache_key("collections", OPENEO_BACKEND_URL)
        collections = _cache_get(cache_key)
        if collections is None:
            collections = conn.list_collections()
            _cache_set(cache_key, collections)

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
        """List processes, optionally filtered by ID/summary substring. Results are cached."""
        conn = get_openeo_connection()

        cache_key = _cache_key("processes", OPENEO_BACKEND_URL)
        processes = _cache_get(cache_key)
        if processes is None:
            processes = conn.list_processes()
            _cache_set(cache_key, processes)

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
            "links": [{"rel": lnk.rel, "href": lnk.href} for lnk in c.links[:10]],
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

    # ── Synchronous execution ───────────────────────────────────────────────────

    elif name == "openeo_execute_sync":
        """
        Execute a process graph synchronously (blocking until result is ready).
        Best for small AOIs / quick computations. Large jobs should use batch mode.
        """
        conn = get_openeo_connection()
        output_format = args.get("output_format", "GTiff")
        output_file = args.get("output_file")

        import openeo
        cube = conn.datacube_from_process_graph(args["process_graph"])

        if output_file:
            # Download result directly to file
            cube.download(output_file, format=output_format)
            return {
                "status": "completed",
                "output_file": output_file,
                "output_format": output_format,
            }
        else:
            # Return metadata only (no download)
            return {
                "status": "completed",
                "output_format": output_format,
                "message": "Process graph validated. Provide output_file to download result.",
            }

    # ── UDF execution ───────────────────────────────────────────────────────────

    elif name == "openeo_run_udf":
        """
        Wrap a UDF in a process graph and submit as a batch job.
        Builds the process graph automatically from collection + bbox + temporal_extent.
        """
        conn = get_openeo_connection()
        import openeo

        lang = args.get("udf_language", "Python")
        bbox_dict = {
            "west": args["bbox"][0], "south": args["bbox"][1],
            "east": args["bbox"][2], "north": args["bbox"][3],
            "crs": "EPSG:4326"
        }

        # Build a standard process graph: load → apply UDF → save
        cube = (
            conn.load_collection(
                args["collection_id"],
                spatial_extent=bbox_dict,
                temporal_extent=args["temporal_extent"],
            )
            .apply_neighborhood(
                process=lambda data: data.run_udf(
                    udf=args["udf_code"],
                    runtime=lang,
                ),
                size=[{"dimension": "x", "value": 128, "unit": "px"},
                      {"dimension": "y", "value": 128, "unit": "px"}],
                overlap=[],
            )
            .save_result(format="GTiff")
        )

        job = cube.create_job(title=args.get("title", f"UDF job ({lang})"))
        job.start_job()

        return {
            "job_id": job.job_id,
            "status": job.status(),
            "language": lang,
            "collection": args["collection_id"],
            "message": "UDF job submitted. Use openeo_job_status to monitor."
        }

    # ── Webhook ─────────────────────────────────────────────────────────────────

    elif name == "openeo_set_job_webhook":
        """
        Register a webhook for a job and start background polling.
        Sends a POST request to webhook_url when job finishes or errors.
        """
        job_id = args["job_id"]
        webhook_url = args["webhook_url"]
        _webhooks[job_id] = webhook_url

        # Start background polling task
        asyncio.create_task(_poll_job_webhook(job_id, webhook_url))

        return {
            "job_id": job_id,
            "webhook_url": webhook_url,
            "status": "webhook_registered",
            "message": "Polling every 60s. Will POST to webhook_url on completion.",
        }

    # ── Cache control ────────────────────────────────────────────────────────────

    elif name == "cache_clear":
        """Clear all cached metadata entries."""
        count = len(_cache)
        _cache.clear()
        return {"cleared_entries": count, "message": "Cache cleared."}

    else:
        raise ValueError(f"Unknown tool: {name!r}. Check list_tools() for available tools.")


# ─── Webhook polling ───────────────────────────────────────────────────────────

async def _poll_job_webhook(job_id: str, webhook_url: str, poll_interval: int = 60):
    """
    Background task: poll an OpenEO job until it finishes or errors,
    then POST the final status to the registered webhook URL.

    Args:
        job_id:        OpenEO job ID to monitor.
        webhook_url:   HTTPS URL to notify on completion.
        poll_interval: Seconds between status checks (default: 60).
    """
    import requests

    terminal_statuses = {"finished", "error", "canceled"}

    while True:
        await asyncio.sleep(poll_interval)

        try:
            conn = get_openeo_connection()
            job = conn.job(job_id)
            info = job.describe_job()
            status = info.get("status", "unknown")

            if status in terminal_statuses:
                # Job is done — notify webhook
                payload = {
                    "job_id": job_id,
                    "status": status,
                    "updated": info.get("updated"),
                    "error": info.get("error"),
                }
                try:
                    requests.post(webhook_url, json=payload, timeout=10)
                except Exception:
                    pass  # Webhook delivery failure is non-fatal

                # Cleanup registry
                _webhooks.pop(job_id, None)
                break

        except Exception:
            # Network error or job not found — keep polling
            pass


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
