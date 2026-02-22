#!/usr/bin/env python3
"""
openeo-mcp: MCP server for OpenEO and STAC APIs
Default backend: Copernicus Data Space Ecosystem (CDSE)
"""

import os
import json
from typing import Any, Optional
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

# Config from env
OPENEO_BACKEND_URL = os.getenv("OPENEO_BACKEND_URL", "https://openeo.dataspace.copernicus.eu")
STAC_API_URL = os.getenv("STAC_API_URL", "https://catalogue.dataspace.copernicus.eu/stac")
OPENEO_TOKEN = os.getenv("OPENEO_TOKEN", "")

server = Server("openeo-mcp")

# ─── Session token store (in-memory, per server instance) ──────────────────────
_session_connection = None  # authenticated openeo.Connection

# ─── OpenEO helpers ────────────────────────────────────────────────────────────

def get_openeo_connection():
    global _session_connection
    import openeo
    if _session_connection is not None:
        return _session_connection
    conn = openeo.connect(OPENEO_BACKEND_URL)
    if OPENEO_TOKEN:
        conn.authenticate_oidc_access_token(OPENEO_TOKEN)
    return conn

def get_stac_client():
    from pystac_client import Client
    return Client.open(STAC_API_URL)

# ─── Tools ─────────────────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="openeo_authenticate",
            description=(
                "Start OIDC Device Flow login with your CDSE account. "
                "Returns a URL — open it in your browser to authenticate. "
                "Once done, all subsequent OpenEO job tools will use your credentials."
            ),
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="openeo_connect",
            description="Test connection to an OpenEO backend and return its capabilities.",
            inputSchema={
                "type": "object",
                "properties": {
                    "backend_url": {
                        "type": "string",
                        "description": f"OpenEO backend URL (default: {OPENEO_BACKEND_URL})"
                    }
                }
            }
        ),
        types.Tool(
            name="openeo_list_collections",
            description="List available collections on the OpenEO backend.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "string",
                        "description": "Optional filter string to search collection IDs/titles"
                    }
                }
            }
        ),
        types.Tool(
            name="openeo_describe_collection",
            description="Get detailed metadata for a specific OpenEO collection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "collection_id": {
                        "type": "string",
                        "description": "Collection ID (e.g. SENTINEL2_L2A)"
                    }
                },
                "required": ["collection_id"]
            }
        ),
        types.Tool(
            name="openeo_list_processes",
            description="List available OpenEO processes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "string",
                        "description": "Optional filter string to search process IDs/summaries"
                    }
                }
            }
        ),
        types.Tool(
            name="openeo_execute_job",
            description="Submit a batch processing job on OpenEO. Requires authentication (OPENEO_TOKEN).",
            inputSchema={
                "type": "object",
                "properties": {
                    "process_graph": {
                        "type": "object",
                        "description": "OpenEO process graph as JSON object"
                    },
                    "title": {
                        "type": "string",
                        "description": "Job title"
                    },
                    "description": {
                        "type": "string",
                        "description": "Job description"
                    }
                },
                "required": ["process_graph"]
            }
        ),
        types.Tool(
            name="openeo_job_status",
            description="Check the status of an OpenEO batch job.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Job ID returned from openeo_execute_job"
                    }
                },
                "required": ["job_id"]
            }
        ),
        types.Tool(
            name="openeo_download_result",
            description="Download results of a finished OpenEO batch job.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Job ID"
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Local directory to download results to"
                    }
                },
                "required": ["job_id", "output_dir"]
            }
        ),
        types.Tool(
            name="stac_list_collections",
            description="List all collections available in the STAC API.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "string",
                        "description": "Optional filter string"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of collections to return (default: 50)"
                    }
                }
            }
        ),
        types.Tool(
            name="stac_search",
            description="Search STAC items by spatial/temporal filters.",
            inputSchema={
                "type": "object",
                "properties": {
                    "collections": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Collection IDs to search"
                    },
                    "bbox": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Bounding box [west, south, east, north]"
                    },
                    "datetime": {
                        "type": "string",
                        "description": "Date range e.g. '2024-01-01/2024-03-31' or single date"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of items (default: 10)"
                    },
                    "query": {
                        "type": "object",
                        "description": "Additional property filters"
                    }
                }
            }
        ),
        types.Tool(
            name="stac_get_collection",
            description="Get metadata for a specific STAC collection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "collection_id": {
                        "type": "string",
                        "description": "STAC collection ID"
                    }
                },
                "required": ["collection_id"]
            }
        ),
        types.Tool(
            name="stac_get_item",
            description="Get a specific STAC item by collection and item ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "collection_id": {
                        "type": "string",
                        "description": "STAC collection ID"
                    },
                    "item_id": {
                        "type": "string",
                        "description": "STAC item ID"
                    }
                },
                "required": ["collection_id", "item_id"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    try:
        result = await _dispatch(name, arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: {type(e).__name__}: {e}")]


async def _dispatch(name: str, args: dict) -> Any:
    # ── OpenEO ──────────────────────────────────────────────────────────────────
    if name == "openeo_authenticate":
        global _session_connection
        import openeo
        conn = openeo.connect(OPENEO_BACKEND_URL)
        # device_code flow: prints URL, waits for user to authenticate in browser
        conn.authenticate_oidc_device(
            client_id="openeo-platform",
            max_poll_time=300,
        )
        _session_connection = conn
        user_info = conn.describe_account()
        return {
            "status": "authenticated",
            "user": user_info.get("user_id", "unknown"),
            "backend": OPENEO_BACKEND_URL,
            "message": "You are now logged in. Job tools will use your CDSE account.",
        }

    elif name == "openeo_connect":
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
        conn = get_openeo_connection()
        collections = conn.list_collections()
        f = args.get("filter", "").lower()
        result = []
        for c in collections:
            cid = c.get("id", "")
            title = c.get("title", "")
            if not f or f in cid.lower() or f in title.lower():
                result.append({
                    "id": cid,
                    "title": title,
                    "description": c.get("description", "")[:200],
                })
        return {"count": len(result), "collections": result}

    elif name == "openeo_describe_collection":
        conn = get_openeo_connection()
        c = conn.describe_collection(args["collection_id"])
        return dict(c)

    elif name == "openeo_list_processes":
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

    elif name == "openeo_execute_job":
        conn = get_openeo_connection()
        job = conn.create_job(
            process_graph=args["process_graph"],
            title=args.get("title", "openeo-mcp job"),
            description=args.get("description", ""),
        )
        job.start_job()
        return {"job_id": job.job_id, "status": job.status(), "title": args.get("title")}

    elif name == "openeo_job_status":
        conn = get_openeo_connection()
        job = conn.job(args["job_id"])
        info = job.describe_job()
        return {
            "job_id": args["job_id"],
            "status": info.get("status"),
            "progress": info.get("progress"),
            "created": info.get("created"),
            "updated": info.get("updated"),
            "error": info.get("error"),
        }

    elif name == "openeo_download_result":
        conn = get_openeo_connection()
        job = conn.job(args["job_id"])
        output_dir = args["output_dir"]
        os.makedirs(output_dir, exist_ok=True)
        results = job.get_results()
        results.download_files(output_dir)
        files = os.listdir(output_dir)
        return {"job_id": args["job_id"], "output_dir": output_dir, "files": files}

    # ── STAC ────────────────────────────────────────────────────────────────────
    elif name == "stac_list_collections":
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
                break
        return {"count": len(collections), "collections": collections}

    elif name == "stac_search":
        client = get_stac_client()
        search_params = {}
        if "collections" in args:
            search_params["collections"] = args["collections"]
        if "bbox" in args:
            search_params["bbox"] = args["bbox"]
        if "datetime" in args:
            search_params["datetime"] = args["datetime"]
        if "query" in args:
            search_params["query"] = args["query"]
        limit = args.get("limit", 10)
        search = client.search(**search_params, max_items=limit)
        items = []
        for item in search.items():
            items.append({
                "id": item.id,
                "collection": item.collection_id,
                "datetime": str(item.datetime),
                "bbox": item.bbox,
                "assets": list(item.assets.keys()),
                "properties": {k: v for k, v in item.properties.items()
                               if k in ["datetime", "platform", "instrument",
                                        "eo:cloud_cover", "s2:mgrs_tile",
                                        "processing:level"]},
            })
        return {"count": len(items), "items": items}

    elif name == "stac_get_collection":
        client = get_stac_client()
        c = client.get_collection(args["collection_id"])
        return {
            "id": c.id,
            "title": c.title,
            "description": c.description,
            "extent": c.extent.to_dict() if c.extent else None,
            "license": c.license,
            "links": [{"rel": l.rel, "href": l.href} for l in c.links[:10]],
        }

    elif name == "stac_get_item":
        import requests
        url = f"{STAC_API_URL}/collections/{args['collection_id']}/items/{args['item_id']}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    else:
        raise ValueError(f"Unknown tool: {name}")


async def main():
    async with stdio_server() as streams:
        await server.run(streams[0], streams[1], server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
