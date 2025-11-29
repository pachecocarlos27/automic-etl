"""Data lineage graph representation and visualization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from enum import Enum
import json

from automic_etl.lineage.tracker import LineageEvent, OperationType, DataAsset


class NodeType(Enum):
    """Types of nodes in lineage graph."""
    SOURCE = "source"
    TRANSFORMATION = "transformation"
    TARGET = "target"
    DATASET = "dataset"


@dataclass
class LineageNode:
    """A node in the lineage graph."""
    id: str
    name: str
    node_type: NodeType
    asset_type: str | None = None
    location: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.node_type.value,
            "asset_type": self.asset_type,
            "location": self.location,
            "metadata": self.metadata,
        }


@dataclass
class LineageEdge:
    """An edge in the lineage graph."""
    source_id: str
    target_id: str
    operation: OperationType
    transformation: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source_id,
            "target": self.target_id,
            "operation": self.operation.value,
            "transformation": self.transformation,
            "metadata": self.metadata,
        }


class LineageGraph:
    """
    Graph representation of data lineage.

    Features:
    - Build graph from lineage events
    - Query upstream/downstream dependencies
    - Export for visualization
    - Impact analysis
    """

    def __init__(self) -> None:
        self.nodes: dict[str, LineageNode] = {}
        self.edges: list[LineageEdge] = []
        self._adjacency: dict[str, list[str]] = {}
        self._reverse_adjacency: dict[str, list[str]] = {}

    def add_node(self, node: LineageNode) -> None:
        """Add a node to the graph."""
        self.nodes[node.id] = node
        if node.id not in self._adjacency:
            self._adjacency[node.id] = []
        if node.id not in self._reverse_adjacency:
            self._reverse_adjacency[node.id] = []

    def add_edge(self, edge: LineageEdge) -> None:
        """Add an edge to the graph."""
        self.edges.append(edge)

        if edge.source_id not in self._adjacency:
            self._adjacency[edge.source_id] = []
        self._adjacency[edge.source_id].append(edge.target_id)

        if edge.target_id not in self._reverse_adjacency:
            self._reverse_adjacency[edge.target_id] = []
        self._reverse_adjacency[edge.target_id].append(edge.source_id)

    def build_from_events(self, events: list[LineageEvent]) -> None:
        """Build graph from lineage events."""
        for event in events:
            # Add source nodes
            for source in event.source_assets:
                node_id = f"asset:{source.name}"
                if node_id not in self.nodes:
                    self.add_node(LineageNode(
                        id=node_id,
                        name=source.name,
                        node_type=NodeType.DATASET,
                        asset_type=source.asset_type,
                        location=source.location,
                    ))

            # Add target nodes
            for target in event.target_assets:
                node_id = f"asset:{target.name}"
                if node_id not in self.nodes:
                    self.add_node(LineageNode(
                        id=node_id,
                        name=target.name,
                        node_type=NodeType.DATASET,
                        asset_type=target.asset_type,
                        location=target.location,
                    ))

            # Add transformation node if applicable
            if event.transformation:
                transform_id = f"transform:{event.event_id}"
                self.add_node(LineageNode(
                    id=transform_id,
                    name=event.transformation[:50],
                    node_type=NodeType.TRANSFORMATION,
                    metadata={"full_transformation": event.transformation},
                ))

                # Connect sources to transformation
                for source in event.source_assets:
                    self.add_edge(LineageEdge(
                        source_id=f"asset:{source.name}",
                        target_id=transform_id,
                        operation=event.operation,
                    ))

                # Connect transformation to targets
                for target in event.target_assets:
                    self.add_edge(LineageEdge(
                        source_id=transform_id,
                        target_id=f"asset:{target.name}",
                        operation=event.operation,
                        transformation=event.transformation,
                    ))
            else:
                # Direct connection without explicit transformation
                for source in event.source_assets:
                    for target in event.target_assets:
                        self.add_edge(LineageEdge(
                            source_id=f"asset:{source.name}",
                            target_id=f"asset:{target.name}",
                            operation=event.operation,
                        ))

    def get_upstream(self, node_id: str, depth: int = -1) -> list[str]:
        """
        Get all upstream nodes.

        Args:
            node_id: Starting node
            depth: Maximum depth (-1 for unlimited)

        Returns:
            List of upstream node IDs
        """
        upstream = []
        visited = set()

        def traverse(nid: str, current_depth: int) -> None:
            if nid in visited:
                return
            if depth >= 0 and current_depth > depth:
                return

            visited.add(nid)

            for parent in self._reverse_adjacency.get(nid, []):
                if parent not in visited:
                    upstream.append(parent)
                    traverse(parent, current_depth + 1)

        traverse(node_id, 0)
        return upstream

    def get_downstream(self, node_id: str, depth: int = -1) -> list[str]:
        """
        Get all downstream nodes.

        Args:
            node_id: Starting node
            depth: Maximum depth (-1 for unlimited)

        Returns:
            List of downstream node IDs
        """
        downstream = []
        visited = set()

        def traverse(nid: str, current_depth: int) -> None:
            if nid in visited:
                return
            if depth >= 0 and current_depth > depth:
                return

            visited.add(nid)

            for child in self._adjacency.get(nid, []):
                if child not in visited:
                    downstream.append(child)
                    traverse(child, current_depth + 1)

        traverse(node_id, 0)
        return downstream

    def get_path(self, source_id: str, target_id: str) -> list[str] | None:
        """
        Find path between two nodes.

        Returns:
            List of node IDs forming the path, or None if no path exists
        """
        if source_id not in self.nodes or target_id not in self.nodes:
            return None

        visited = set()
        queue = [(source_id, [source_id])]

        while queue:
            current, path = queue.pop(0)
            if current == target_id:
                return path

            if current in visited:
                continue
            visited.add(current)

            for neighbor in self._adjacency.get(current, []):
                if neighbor not in visited:
                    queue.append((neighbor, path + [neighbor]))

        return None

    def impact_analysis(self, node_id: str) -> dict[str, Any]:
        """
        Analyze impact of changes to a node.

        Returns:
            Impact analysis including affected nodes
        """
        downstream = self.get_downstream(node_id)

        datasets_affected = [
            self.nodes[nid].name
            for nid in downstream
            if nid in self.nodes and self.nodes[nid].node_type == NodeType.DATASET
        ]

        transformations_affected = [
            self.nodes[nid].name
            for nid in downstream
            if nid in self.nodes and self.nodes[nid].node_type == NodeType.TRANSFORMATION
        ]

        return {
            "source_node": node_id,
            "total_downstream": len(downstream),
            "datasets_affected": datasets_affected,
            "transformations_affected": transformations_affected,
            "downstream_nodes": downstream,
        }

    def get_roots(self) -> list[str]:
        """Get root nodes (nodes with no upstream dependencies)."""
        return [
            node_id for node_id in self.nodes
            if not self._reverse_adjacency.get(node_id)
        ]

    def get_leaves(self) -> list[str]:
        """Get leaf nodes (nodes with no downstream dependencies)."""
        return [
            node_id for node_id in self.nodes
            if not self._adjacency.get(node_id)
        ]

    def to_dict(self) -> dict[str, Any]:
        """Convert graph to dictionary format."""
        return {
            "nodes": [n.to_dict() for n in self.nodes.values()],
            "edges": [e.to_dict() for e in self.edges],
        }

    def to_json(self, path: str | None = None) -> str:
        """Export graph to JSON."""
        data = json.dumps(self.to_dict(), indent=2)
        if path:
            with open(path, "w") as f:
                f.write(data)
        return data

    def to_mermaid(self) -> str:
        """
        Export graph as Mermaid diagram.

        Returns:
            Mermaid diagram string
        """
        lines = ["flowchart LR"]

        # Add nodes
        for node in self.nodes.values():
            if node.node_type == NodeType.DATASET:
                lines.append(f"    {node.id.replace(':', '_')}[({node.name})]")
            elif node.node_type == NodeType.TRANSFORMATION:
                lines.append(f"    {node.id.replace(':', '_')}{{{node.name}}}")
            else:
                lines.append(f"    {node.id.replace(':', '_')}[{node.name}]")

        # Add edges
        for edge in self.edges:
            source = edge.source_id.replace(":", "_")
            target = edge.target_id.replace(":", "_")
            label = edge.operation.value if edge.operation else ""
            lines.append(f"    {source} -->|{label}| {target}")

        return "\n".join(lines)

    def to_dot(self) -> str:
        """
        Export graph as DOT (Graphviz) format.

        Returns:
            DOT format string
        """
        lines = [
            "digraph lineage {",
            "    rankdir=LR;",
            "    node [shape=box];",
        ]

        # Add nodes with styling
        for node in self.nodes.values():
            safe_id = node.id.replace(":", "_").replace("-", "_")
            if node.node_type == NodeType.DATASET:
                lines.append(f'    {safe_id} [label="{node.name}", shape=cylinder];')
            elif node.node_type == NodeType.TRANSFORMATION:
                lines.append(f'    {safe_id} [label="{node.name}", shape=box, style=rounded];')
            else:
                lines.append(f'    {safe_id} [label="{node.name}"];')

        # Add edges
        for edge in self.edges:
            source = edge.source_id.replace(":", "_").replace("-", "_")
            target = edge.target_id.replace(":", "_").replace("-", "_")
            label = edge.operation.value if edge.operation else ""
            lines.append(f'    {source} -> {target} [label="{label}"];')

        lines.append("}")
        return "\n".join(lines)

    def to_cytoscape(self) -> dict[str, Any]:
        """
        Export graph in Cytoscape.js format for web visualization.

        Returns:
            Cytoscape.js compatible data structure
        """
        elements = {"nodes": [], "edges": []}

        for node in self.nodes.values():
            elements["nodes"].append({
                "data": {
                    "id": node.id,
                    "label": node.name,
                    "type": node.node_type.value,
                }
            })

        for i, edge in enumerate(self.edges):
            elements["edges"].append({
                "data": {
                    "id": f"edge_{i}",
                    "source": edge.source_id,
                    "target": edge.target_id,
                    "operation": edge.operation.value,
                }
            })

        return elements

    def summary(self) -> dict[str, Any]:
        """Get graph summary statistics."""
        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "datasets": sum(1 for n in self.nodes.values() if n.node_type == NodeType.DATASET),
            "transformations": sum(1 for n in self.nodes.values() if n.node_type == NodeType.TRANSFORMATION),
            "root_nodes": len(self.get_roots()),
            "leaf_nodes": len(self.get_leaves()),
        }
