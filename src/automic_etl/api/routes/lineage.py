"""Data lineage endpoints."""

from __future__ import annotations

from typing import Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from automic_etl.api.models import (
    LineageGraph,
    LineageNode,
    LineageEdge,
    ImpactAnalysis,
    DataTier,
)

router = APIRouter()

# Sample lineage data for demo
_lineage_nodes = {
    "source.salesforce.accounts": LineageNode(
        id="source.salesforce.accounts",
        name="Salesforce Accounts",
        type="source",
        metadata={"connector": "salesforce", "object": "Account"}
    ),
    "source.postgres.orders": LineageNode(
        id="source.postgres.orders",
        name="PostgreSQL Orders",
        type="source",
        metadata={"connector": "postgres", "table": "orders"}
    ),
    "bronze.raw_accounts": LineageNode(
        id="bronze.raw_accounts",
        name="raw_accounts",
        type="table",
        tier=DataTier.BRONZE,
        metadata={"format": "delta", "row_count": 50000}
    ),
    "bronze.raw_orders": LineageNode(
        id="bronze.raw_orders",
        name="raw_orders",
        type="table",
        tier=DataTier.BRONZE,
        metadata={"format": "delta", "row_count": 250000}
    ),
    "silver.customers": LineageNode(
        id="silver.customers",
        name="customers",
        type="table",
        tier=DataTier.SILVER,
        metadata={"format": "delta", "row_count": 45000}
    ),
    "silver.orders": LineageNode(
        id="silver.orders",
        name="orders",
        type="table",
        tier=DataTier.SILVER,
        metadata={"format": "delta", "row_count": 240000}
    ),
    "gold.customer_360": LineageNode(
        id="gold.customer_360",
        name="customer_360",
        type="table",
        tier=DataTier.GOLD,
        metadata={"format": "delta", "row_count": 45000}
    ),
    "gold.sales_summary": LineageNode(
        id="gold.sales_summary",
        name="sales_summary",
        type="table",
        tier=DataTier.GOLD,
        metadata={"format": "delta", "row_count": 1200}
    ),
    "pipeline.ingest_accounts": LineageNode(
        id="pipeline.ingest_accounts",
        name="ingest_accounts",
        type="pipeline",
        metadata={"schedule": "@daily", "last_run": "2024-01-15T08:00:00Z"}
    ),
    "pipeline.transform_customers": LineageNode(
        id="pipeline.transform_customers",
        name="transform_customers",
        type="pipeline",
        metadata={"schedule": "@daily", "last_run": "2024-01-15T09:00:00Z"}
    ),
}

_lineage_edges = [
    LineageEdge(
        source="source.salesforce.accounts",
        target="bronze.raw_accounts",
        type="feeds_into",
        pipeline="ingest_accounts"
    ),
    LineageEdge(
        source="source.postgres.orders",
        target="bronze.raw_orders",
        type="feeds_into",
        pipeline="ingest_orders"
    ),
    LineageEdge(
        source="bronze.raw_accounts",
        target="silver.customers",
        type="transforms",
        pipeline="transform_customers"
    ),
    LineageEdge(
        source="bronze.raw_orders",
        target="silver.orders",
        type="transforms",
        pipeline="transform_orders"
    ),
    LineageEdge(
        source="silver.customers",
        target="gold.customer_360",
        type="derives_from",
        pipeline="build_customer_360"
    ),
    LineageEdge(
        source="silver.orders",
        target="gold.customer_360",
        type="derives_from",
        pipeline="build_customer_360"
    ),
    LineageEdge(
        source="silver.orders",
        target="gold.sales_summary",
        type="derives_from",
        pipeline="aggregate_sales"
    ),
]


@router.get("/graph", response_model=LineageGraph)
async def get_lineage_graph(
    tier: DataTier | None = None,
    include_sources: bool = True,
    include_pipelines: bool = True,
):
    """
    Get the complete lineage graph.

    Args:
        tier: Filter by data tier
        include_sources: Include external sources
        include_pipelines: Include pipeline nodes
    """
    nodes = []
    for node in _lineage_nodes.values():
        # Filter by tier
        if tier and node.tier and node.tier != tier:
            continue
        # Filter sources
        if not include_sources and node.type == "source":
            continue
        # Filter pipelines
        if not include_pipelines and node.type == "pipeline":
            continue
        nodes.append(node)

    # Get relevant edges
    node_ids = {n.id for n in nodes}
    edges = [
        e for e in _lineage_edges
        if e.source in node_ids and e.target in node_ids
    ]

    return LineageGraph(nodes=nodes, edges=edges)


@router.get("/table/{table_fqn}", response_model=LineageGraph)
async def get_table_lineage(
    table_fqn: str,
    direction: str = Query("both", regex="^(upstream|downstream|both)$"),
    depth: int = Query(3, ge=1, le=10),
):
    """
    Get lineage for a specific table.

    Args:
        table_fqn: Fully qualified table name (e.g., 'silver.customers')
        direction: Lineage direction (upstream, downstream, both)
        depth: How many levels to traverse
    """
    if table_fqn not in _lineage_nodes:
        raise HTTPException(status_code=404, detail=f"Table '{table_fqn}' not found in lineage")

    visited = set()
    nodes = []
    edges = []

    def traverse_upstream(node_id: str, current_depth: int):
        if current_depth > depth or node_id in visited:
            return
        visited.add(node_id)

        if node_id in _lineage_nodes:
            nodes.append(_lineage_nodes[node_id])

        for edge in _lineage_edges:
            if edge.target == node_id:
                edges.append(edge)
                traverse_upstream(edge.source, current_depth + 1)

    def traverse_downstream(node_id: str, current_depth: int):
        if current_depth > depth or node_id in visited:
            return
        visited.add(node_id)

        if node_id in _lineage_nodes:
            nodes.append(_lineage_nodes[node_id])

        for edge in _lineage_edges:
            if edge.source == node_id:
                edges.append(edge)
                traverse_downstream(edge.target, current_depth + 1)

    # Start traversal
    if direction in ("upstream", "both"):
        traverse_upstream(table_fqn, 0)

    visited.clear()  # Reset for downstream

    if direction in ("downstream", "both"):
        traverse_downstream(table_fqn, 0)

    # Deduplicate nodes
    unique_nodes = {n.id: n for n in nodes}
    unique_edges = list({(e.source, e.target): e for e in edges}.values())

    return LineageGraph(nodes=list(unique_nodes.values()), edges=unique_edges)


@router.get("/impact/{table_fqn}", response_model=ImpactAnalysis)
async def analyze_impact(
    table_fqn: str,
    change_type: str = Query("schema", regex="^(schema|data|delete)$"),
):
    """
    Analyze the impact of changes to a table.

    Args:
        table_fqn: Fully qualified table name
        change_type: Type of change (schema, data, delete)
    """
    if table_fqn not in _lineage_nodes:
        raise HTTPException(status_code=404, detail=f"Table '{table_fqn}' not found")

    # Find all downstream tables and pipelines
    downstream_tables = set()
    downstream_pipelines = set()
    visited = set()

    def find_downstream(node_id: str):
        if node_id in visited:
            return
        visited.add(node_id)

        for edge in _lineage_edges:
            if edge.source == node_id:
                target = edge.target
                if edge.pipeline:
                    downstream_pipelines.add(edge.pipeline)

                if target in _lineage_nodes:
                    node = _lineage_nodes[target]
                    if node.type == "table":
                        downstream_tables.add(target)

                find_downstream(target)

    find_downstream(table_fqn)

    # Determine risk level
    downstream_count = len(downstream_tables)
    if downstream_count == 0:
        risk = "low"
    elif downstream_count <= 3:
        risk = "medium"
    else:
        risk = "high"

    if change_type == "delete":
        risk = "high"

    return ImpactAnalysis(
        source=table_fqn,
        impacted_tables=list(downstream_tables),
        impacted_pipelines=list(downstream_pipelines),
        downstream_count=downstream_count,
        risk_level=risk,
    )


@router.get("/pipeline/{pipeline_name}")
async def get_pipeline_lineage(pipeline_name: str):
    """
    Get lineage for a specific pipeline.

    Args:
        pipeline_name: Pipeline name
    """
    # Find edges involving this pipeline
    input_tables = []
    output_tables = []

    for edge in _lineage_edges:
        if edge.pipeline == pipeline_name:
            input_tables.append(edge.source)
            output_tables.append(edge.target)

    return {
        "pipeline": pipeline_name,
        "input_tables": list(set(input_tables)),
        "output_tables": list(set(output_tables)),
        "edge_count": len([e for e in _lineage_edges if e.pipeline == pipeline_name]),
    }


@router.post("/edge")
async def add_lineage_edge(
    source: str,
    target: str,
    edge_type: str = "derives_from",
    pipeline: str | None = None,
):
    """
    Add a lineage edge.

    Args:
        source: Source entity
        target: Target entity
        edge_type: Edge type
        pipeline: Associated pipeline
    """
    edge = LineageEdge(
        source=source,
        target=target,
        type=edge_type,
        pipeline=pipeline,
    )
    _lineage_edges.append(edge)

    return {"success": True, "edge": edge}


@router.delete("/edge")
async def delete_lineage_edge(source: str, target: str):
    """
    Delete a lineage edge.

    Args:
        source: Source entity
        target: Target entity
    """
    global _lineage_edges

    original_count = len(_lineage_edges)
    _lineage_edges = [
        e for e in _lineage_edges
        if not (e.source == source and e.target == target)
    ]

    if len(_lineage_edges) == original_count:
        raise HTTPException(status_code=404, detail="Edge not found")

    return {"success": True, "message": "Edge deleted"}


@router.get("/stats")
async def get_lineage_stats():
    """Get lineage statistics."""
    node_counts = {"source": 0, "table": 0, "pipeline": 0, "destination": 0}
    tier_counts = {"bronze": 0, "silver": 0, "gold": 0}

    for node in _lineage_nodes.values():
        node_counts[node.type] = node_counts.get(node.type, 0) + 1
        if node.tier:
            tier_counts[node.tier.value] = tier_counts.get(node.tier.value, 0) + 1

    return {
        "total_nodes": len(_lineage_nodes),
        "total_edges": len(_lineage_edges),
        "node_counts": node_counts,
        "tier_counts": tier_counts,
        "orphan_tables": 0,  # Tables with no lineage
        "max_depth": 4,  # Maximum lineage depth
    }
