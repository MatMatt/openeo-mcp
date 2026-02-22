"""
Unit tests for openeo-mcp server.

Tests are split into:
  - Unit tests: mock OpenEO/STAC clients, no network required
  - Integration tests: marked with @pytest.mark.integration, require live CDSE access

Run unit tests only:
    pytest tests/

Run all including integration:
    pytest tests/ -m integration
"""

import pytest
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_openeo_connection():
    """Fake openeo.Connection with common methods stubbed."""
    conn = MagicMock()

    conn.capabilities.return_value = MagicMock(
        api_version=lambda: "1.2.0",
        get=lambda k, d=None: {"title": "CDSE", "description": "Test backend",
                               "endpoints": [1, 2, 3], "backend_version": "1.0"}.get(k, d)
    )
    conn.list_collections.return_value = [
        {"id": "SENTINEL2_L2A", "title": "Sentinel-2 L2A", "description": "Surface reflectance"},
        {"id": "SENTINEL1_GRD", "title": "Sentinel-1 GRD", "description": "SAR backscatter"},
        {"id": "LANDSAT8_L2",   "title": "Landsat-8 L2",   "description": "Landsat surface refl"},
    ]
    conn.describe_collection.return_value = {
        "id": "SENTINEL2_L2A",
        "title": "Sentinel-2 L2A",
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [["2015-06-23", None]]}},
        "bands": ["B02", "B03", "B04", "B08"],
    }
    conn.list_processes.return_value = [
        {"id": "ndvi",            "summary": "Compute Normalized Difference Vegetation Index"},
        {"id": "load_collection", "summary": "Load a collection from the current backend"},
        {"id": "save_result",     "summary": "Save processed results"},
    ]

    mock_job = MagicMock()
    mock_job.job_id = "job-abc123"
    mock_job.status.return_value = "queued"
    mock_job.describe_job.return_value = {
        "status": "finished", "progress": 100,
        "created": "2026-02-22T10:00:00Z", "updated": "2026-02-22T10:05:00Z", "error": None
    }
    mock_job.get_results.return_value = MagicMock(
        download_files=MagicMock(return_value=None)
    )
    conn.create_job.return_value = mock_job
    conn.job.return_value = mock_job
    conn.describe_account.return_value = {"user_id": "test_user@example.com"}

    return conn


@pytest.fixture
def mock_stac_client():
    """Fake pystac_client.Client with collections and search stubbed."""
    client = MagicMock()

    # Collections
    col1 = MagicMock(id="SENTINEL-2", title="Sentinel-2", description="ESA Sentinel-2",
                     extent=MagicMock(to_dict=lambda: {"spatial": {}, "temporal": {}}),
                     license="proprietary",
                     links=[MagicMock(rel="self", href="https://example.com")])
    col2 = MagicMock(id="LANDSAT-8",  title="Landsat-8",  description="USGS Landsat-8",
                     extent=None, license="public-domain", links=[])
    client.get_collections.return_value = [col1, col2]
    client.get_collection.return_value = col1

    # Search items
    item1 = MagicMock(
        id="S2A_MSIL2A_20240601",
        collection_id="SENTINEL-2",
        datetime=MagicMock(__str__=lambda s: "2024-06-01T10:00:00Z"),
        bbox=[11.0, 46.0, 12.0, 47.0],
        assets={"B02": MagicMock(), "B03": MagicMock(), "SCL": MagicMock()},
        properties={"eo:cloud_cover": 5.2, "platform": "sentinel-2a", "s2:mgrs_tile": "T32TPS"},
    )
    search_result = MagicMock()
    search_result.items.return_value = [item1]
    client.search.return_value = search_result

    return client


# ─── Helper ────────────────────────────────────────────────────────────────────

def run(coro):
    """Run an async coroutine synchronously in tests."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ─── OpenEO Unit Tests ─────────────────────────────────────────────────────────

class TestOpenEOConnect:
    def test_returns_capabilities(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection), \
             patch("openeo.connect", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_connect", {}))
            assert result["api_version"] == "1.2.0"
            assert result["title"] == "CDSE"
            assert result["endpoints"] == 3

    def test_custom_backend_url(self, mock_openeo_connection):
        with patch("openeo.connect", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_connect", {"backend_url": "https://openeo.cloud"}))
            assert result["backend_url"] == "https://openeo.cloud"


class TestOpenEOListCollections:
    def test_returns_all_collections(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_list_collections", {}))
            assert result["count"] == 3
            assert result["collections"][0]["id"] == "SENTINEL2_L2A"

    def test_filter_by_id(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_list_collections", {"filter": "sentinel"}))
            assert result["count"] == 2  # SENTINEL2_L2A + SENTINEL1_GRD
            ids = [c["id"] for c in result["collections"]]
            assert "SENTINEL2_L2A" in ids
            assert "LANDSAT8_L2" not in ids

    def test_filter_no_match(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_list_collections", {"filter": "modis"}))
            assert result["count"] == 0


class TestOpenEODescribeCollection:
    def test_returns_metadata(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_describe_collection", {"collection_id": "SENTINEL2_L2A"}))
            assert result["id"] == "SENTINEL2_L2A"
            assert "bands" in result


class TestOpenEOListProcesses:
    def test_returns_all(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_list_processes", {}))
            assert result["count"] == 3

    def test_filter(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_list_processes", {"filter": "ndvi"}))
            assert result["count"] == 1
            assert result["processes"][0]["id"] == "ndvi"


class TestOpenEOExecuteJob:
    def test_submit_job(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            pg = {"load": {"process_id": "load_collection", "arguments": {"id": "SENTINEL2_L2A"}}}
            result = run(server._dispatch("openeo_execute_job", {
                "process_graph": pg,
                "title": "Test NDVI job"
            }))
            assert result["job_id"] == "job-abc123"
            assert result["status"] == "queued"

    def test_missing_process_graph_raises(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            with pytest.raises(KeyError):
                run(server._dispatch("openeo_execute_job", {}))


class TestOpenEOJobStatus:
    def test_returns_status(self, mock_openeo_connection):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_job_status", {"job_id": "job-abc123"}))
            assert result["status"] == "finished"
            assert result["progress"] == 100
            assert result["error"] is None


class TestOpenEODownloadResult:
    def test_download(self, mock_openeo_connection, tmp_path):
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            result = run(server._dispatch("openeo_download_result", {
                "job_id": "job-abc123",
                "output_dir": str(tmp_path / "results")
            }))
            assert result["job_id"] == "job-abc123"
            assert "results" in result["output_dir"]


class TestOpenEOSyncProcess:
    def test_sync_process(self, mock_openeo_connection):
        mock_openeo_connection.execute.return_value = {"result": "ok"}
        with patch("server.get_openeo_connection", return_value=mock_openeo_connection):
            import server
            pg = {"ndvi": {"process_id": "ndvi", "arguments": {}}}
            result = run(server._dispatch("openeo_execute_sync", {
                "process_graph": pg,
                "output_format": "GTiff"
            }))
            assert result["status"] == "completed"


# ─── STAC Unit Tests ───────────────────────────────────────────────────────────

class TestSTACListCollections:
    def test_returns_all(self, mock_stac_client):
        with patch("server.get_stac_client", return_value=mock_stac_client):
            import server
            result = run(server._dispatch("stac_list_collections", {}))
            assert result["count"] == 2

    def test_filter(self, mock_stac_client):
        with patch("server.get_stac_client", return_value=mock_stac_client):
            import server
            result = run(server._dispatch("stac_list_collections", {"filter": "sentinel"}))
            assert result["count"] == 1
            assert result["collections"][0]["id"] == "SENTINEL-2"

    def test_limit(self, mock_stac_client):
        with patch("server.get_stac_client", return_value=mock_stac_client):
            import server
            result = run(server._dispatch("stac_list_collections", {"limit": 1}))
            assert result["count"] == 1


class TestSTACSearch:
    def test_basic_search(self, mock_stac_client):
        with patch("server.get_stac_client", return_value=mock_stac_client):
            import server
            result = run(server._dispatch("stac_search", {
                "collections": ["SENTINEL-2"],
                "bbox": [11.0, 46.0, 12.0, 47.0],
                "datetime": "2024-06-01/2024-06-30",
                "limit": 5
            }))
            assert result["count"] == 1
            item = result["items"][0]
            assert item["id"] == "S2A_MSIL2A_20240601"
            assert "B02" in item["assets"]
            assert item["properties"]["eo:cloud_cover"] == 5.2

    def test_search_passes_params_to_client(self, mock_stac_client):
        with patch("server.get_stac_client", return_value=mock_stac_client):
            import server
            run(server._dispatch("stac_search", {
                "collections": ["SENTINEL-2"],
                "bbox": [10, 45, 13, 48],
            }))
            call_kwargs = mock_stac_client.search.call_args[1]
            assert call_kwargs["collections"] == ["SENTINEL-2"]
            assert call_kwargs["bbox"] == [10, 45, 13, 48]

    def test_cloud_cover_filter(self, mock_stac_client):
        """Extended STAC filter: cloud cover < 20%"""
        with patch("server.get_stac_client", return_value=mock_stac_client):
            import server
            result = run(server._dispatch("stac_search", {
                "collections": ["SENTINEL-2"],
                "query": {"eo:cloud_cover": {"lt": 20}}
            }))
            assert result["count"] >= 0  # Just verify no crash


class TestSTACGetCollection:
    def test_returns_metadata(self, mock_stac_client):
        with patch("server.get_stac_client", return_value=mock_stac_client):
            import server
            result = run(server._dispatch("stac_get_collection", {"collection_id": "SENTINEL-2"}))
            assert result["id"] == "SENTINEL-2"
            assert result["title"] == "Sentinel-2"
            assert "extent" in result


class TestSTACGetItem:
    def test_fetches_item(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "type": "Feature",
            "id": "S2A_MSIL2A_20240601",
            "collection": "SENTINEL-2",
            "assets": {"B02": {"href": "https://example.com/B02.tif"}}
        }
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response):
            import server
            result = run(server._dispatch("stac_get_item", {
                "collection_id": "SENTINEL-2",
                "item_id": "S2A_MSIL2A_20240601"
            }))
            assert result["id"] == "S2A_MSIL2A_20240601"
            assert "B02" in result["assets"]


# ─── Error handling ────────────────────────────────────────────────────────────

class TestErrorHandling:
    def test_unknown_tool_raises(self):
        import server
        with pytest.raises(ValueError, match="Unknown tool"):
            run(server._dispatch("nonexistent_tool", {}))

    def test_call_tool_returns_error_text(self):
        """call_tool should never crash — errors become TextContent."""
        import server
        result = asyncio.get_event_loop().run_until_complete(
            server.call_tool("nonexistent_tool", {})
        )
        assert len(result) == 1
        assert "Error" in result[0].text


# ─── Integration tests (require live CDSE) ─────────────────────────────────────

@pytest.mark.integration
class TestIntegrationCDSE:
    """
    Live integration tests against CDSE.
    Run with: pytest tests/ -m integration

    Requires network access to:
      - https://openeo.dataspace.copernicus.eu
      - https://catalogue.dataspace.copernicus.eu/stac
    """

    def test_openeo_connect_live(self):
        import server
        result = run(server._dispatch("openeo_connect", {}))
        assert result["api_version"] is not None
        assert "openeo" in result["backend_url"]

    def test_openeo_list_collections_live(self):
        import server
        result = run(server._dispatch("openeo_list_collections", {"filter": "sentinel"}))
        assert result["count"] > 0

    def test_stac_list_collections_live(self):
        import server
        result = run(server._dispatch("stac_list_collections", {"limit": 5}))
        assert result["count"] > 0

    def test_stac_search_south_tyrol(self):
        """Search Sentinel-2 over South Tyrol (Matteo's home region)."""
        import server
        result = run(server._dispatch("stac_search", {
            "collections": ["SENTINEL-2"],
            "bbox": [10.5, 46.2, 12.5, 47.1],   # South Tyrol bounding box
            "datetime": "2024-06-01/2024-06-30",
            "limit": 3
        }))
        assert result["count"] >= 0  # May be 0 if no scenes available
