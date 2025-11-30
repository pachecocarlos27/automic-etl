"""Unified global search for Automic ETL UI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from datetime import datetime
import streamlit as st

from automic_etl.ui.state import get_state, SearchResult


@dataclass
class SearchCategory:
    """Search category definition."""
    key: str
    label: str
    icon: str
    search_func: Callable[[str], list[SearchResult]]


class GlobalSearch:
    """
    Global search functionality across all data.

    Searches:
    - Tables (bronze, silver, gold)
    - Pipelines
    - Connectors
    - Queries
    - Settings
    - Documentation
    """

    def __init__(self):
        self.categories: list[SearchCategory] = [
            SearchCategory("all", "All", "🔍", self._search_all),
            SearchCategory("tables", "Tables", "📊", self._search_tables),
            SearchCategory("pipelines", "Pipelines", "🔧", self._search_pipelines),
            SearchCategory("connectors", "Connectors", "🔌", self._search_connectors),
            SearchCategory("queries", "Queries", "💬", self._search_queries),
            SearchCategory("docs", "Documentation", "📚", self._search_docs),
        ]

        # Sample data for search
        self._tables = [
            {"name": "bronze.raw_customers", "desc": "Raw customer data from CRM", "tier": "bronze", "rows": 125000, "updated": "2 min ago"},
            {"name": "bronze.raw_orders", "desc": "Raw order transactions", "tier": "bronze", "rows": 450000, "updated": "5 min ago"},
            {"name": "bronze.raw_products", "desc": "Product catalog from ERP", "tier": "bronze", "rows": 5000, "updated": "1 hr ago"},
            {"name": "bronze.raw_events", "desc": "User event stream", "tier": "bronze", "rows": 2500000, "updated": "1 min ago"},
            {"name": "silver.customers", "desc": "Cleaned and validated customer data", "tier": "silver", "rows": 120000, "updated": "10 min ago"},
            {"name": "silver.orders", "desc": "Validated orders with enrichment", "tier": "silver", "rows": 445000, "updated": "15 min ago"},
            {"name": "silver.products", "desc": "Standardized product catalog", "tier": "silver", "rows": 4800, "updated": "1 hr ago"},
            {"name": "gold.customer_360", "desc": "Complete customer view with all attributes", "tier": "gold", "rows": 120000, "updated": "30 min ago"},
            {"name": "gold.sales_metrics", "desc": "Aggregated sales KPIs by day/week/month", "tier": "gold", "rows": 36500, "updated": "1 hr ago"},
            {"name": "gold.product_analytics", "desc": "Product performance analytics", "tier": "gold", "rows": 15000, "updated": "2 hr ago"},
        ]

        self._pipelines = [
            {"name": "customer_processing", "desc": "Process and clean customer data", "status": "running", "schedule": "Every 5 min"},
            {"name": "orders_etl", "desc": "Order data extraction and transformation", "status": "completed", "schedule": "Every 15 min"},
            {"name": "daily_aggregation", "desc": "Daily metrics aggregation", "status": "scheduled", "schedule": "Daily at 2 AM"},
            {"name": "data_quality_check", "desc": "Run data quality validations", "status": "failed", "schedule": "Every hour"},
            {"name": "customer_360_build", "desc": "Build customer 360 view", "status": "completed", "schedule": "Every 30 min"},
            {"name": "product_sync", "desc": "Sync products from ERP", "status": "completed", "schedule": "Daily at 6 AM"},
        ]

        self._connectors = [
            {"name": "PostgreSQL - Production", "type": "Database", "status": "connected"},
            {"name": "PostgreSQL - Analytics", "type": "Database", "status": "connected"},
            {"name": "Snowflake", "type": "Data Warehouse", "status": "connected"},
            {"name": "Salesforce", "type": "CRM API", "status": "connected"},
            {"name": "HubSpot", "type": "Marketing API", "status": "disconnected"},
            {"name": "AWS S3 - Data Lake", "type": "Cloud Storage", "status": "connected"},
            {"name": "GCS - Backup", "type": "Cloud Storage", "status": "connected"},
            {"name": "Stripe", "type": "Payment API", "status": "connected"},
        ]

        self._queries = [
            {"name": "Top customers by revenue", "type": "NL Query", "created": "Today"},
            {"name": "Monthly sales trend", "type": "SQL", "created": "Yesterday"},
            {"name": "Product category breakdown", "type": "NL Query", "created": "2 days ago"},
            {"name": "Customer churn analysis", "type": "SQL", "created": "Last week"},
            {"name": "Order fulfillment rate", "type": "NL Query", "created": "Last week"},
        ]

        self._docs = [
            {"name": "Getting Started Guide", "section": "Basics", "keywords": "setup install configure"},
            {"name": "Medallion Architecture", "section": "Concepts", "keywords": "bronze silver gold layers"},
            {"name": "Pipeline Configuration", "section": "Pipelines", "keywords": "schedule transform extract"},
            {"name": "LLM Integration", "section": "AI", "keywords": "natural language query schema inference"},
            {"name": "Data Quality Rules", "section": "Validation", "keywords": "rules validation quality check"},
            {"name": "Connector Setup", "section": "Connectors", "keywords": "database api cloud storage"},
            {"name": "SCD Type 2", "section": "Advanced", "keywords": "slowly changing dimension history"},
            {"name": "Troubleshooting", "section": "Help", "keywords": "error fix debug issue"},
        ]

    def search(self, query: str, category: str = "all") -> list[SearchResult]:
        """
        Perform search across specified category.

        Args:
            query: Search query
            category: Category to search in

        Returns:
            List of search results
        """
        if not query or len(query) < 2:
            return []

        query_lower = query.lower()

        cat = next((c for c in self.categories if c.key == category), None)
        if cat:
            return cat.search_func(query_lower)

        return self._search_all(query_lower)

    def _search_all(self, query: str) -> list[SearchResult]:
        """Search across all categories."""
        results = []
        results.extend(self._search_tables(query)[:5])
        results.extend(self._search_pipelines(query)[:3])
        results.extend(self._search_connectors(query)[:3])
        results.extend(self._search_queries(query)[:3])
        results.extend(self._search_docs(query)[:3])
        return results

    def _search_tables(self, query: str) -> list[SearchResult]:
        """Search tables."""
        results = []
        for table in self._tables:
            if query in table["name"].lower() or query in table["desc"].lower():
                results.append(SearchResult(
                    type="table",
                    title=table["name"],
                    subtitle=f"{table['desc']} • {table['rows']:,} rows • {table['updated']}",
                    page="profiling",
                    icon={"bronze": "🥉", "silver": "🥈", "gold": "🥇"}.get(table["tier"], "📊"),
                    metadata={"tier": table["tier"], "rows": table["rows"]}
                ))
        return results

    def _search_pipelines(self, query: str) -> list[SearchResult]:
        """Search pipelines."""
        results = []
        for pipeline in self._pipelines:
            if query in pipeline["name"].lower() or query in pipeline["desc"].lower():
                status_icon = {
                    "running": "🟢",
                    "completed": "✅",
                    "failed": "🔴",
                    "scheduled": "⏰",
                }.get(pipeline["status"], "⚪")
                results.append(SearchResult(
                    type="pipeline",
                    title=pipeline["name"],
                    subtitle=f"{pipeline['desc']} • {status_icon} {pipeline['status']} • {pipeline['schedule']}",
                    page="pipelines",
                    icon="🔧",
                    metadata={"status": pipeline["status"]}
                ))
        return results

    def _search_connectors(self, query: str) -> list[SearchResult]:
        """Search connectors."""
        results = []
        for conn in self._connectors:
            if query in conn["name"].lower() or query in conn["type"].lower():
                status_icon = "🟢" if conn["status"] == "connected" else "🔴"
                results.append(SearchResult(
                    type="connector",
                    title=conn["name"],
                    subtitle=f"{conn['type']} • {status_icon} {conn['status']}",
                    page="connectors",
                    icon="🔌",
                    metadata={"status": conn["status"]}
                ))
        return results

    def _search_queries(self, query: str) -> list[SearchResult]:
        """Search saved queries."""
        results = []
        for q in self._queries:
            if query in q["name"].lower():
                results.append(SearchResult(
                    type="query",
                    title=q["name"],
                    subtitle=f"{q['type']} • Created {q['created']}",
                    page="query",
                    icon="💬",
                ))
        return results

    def _search_docs(self, query: str) -> list[SearchResult]:
        """Search documentation."""
        results = []
        for doc in self._docs:
            if query in doc["name"].lower() or query in doc["keywords"].lower():
                results.append(SearchResult(
                    type="doc",
                    title=doc["name"],
                    subtitle=f"Documentation • {doc['section']}",
                    page="help",
                    icon="📚",
                ))
        return results


# Global search instance
_global_search: GlobalSearch | None = None


def get_global_search() -> GlobalSearch:
    """Get global search instance."""
    global _global_search
    if _global_search is None:
        _global_search = GlobalSearch()
    return _global_search


def render_search_bar(
    placeholder: str = "Search tables, pipelines, connectors...",
    key: str = "global_search",
):
    """
    Render the global search bar.

    Args:
        placeholder: Search input placeholder
        key: Streamlit widget key
    """
    state = get_state()

    col1, col2 = st.columns([6, 1])

    with col1:
        query = st.text_input(
            label="Search",
            placeholder=placeholder,
            key=key,
            label_visibility="collapsed",
        )

    with col2:
        if st.button("🔍", key=f"{key}_btn", use_container_width=True):
            pass  # Search triggered by input

    if query and len(query) >= 2:
        search = get_global_search()
        results = search.search(query)
        state.set("search_results", results)

        if results:
            render_search_results(results)
        else:
            st.info("No results found")


def render_search_results(results: list[SearchResult], max_results: int = 10):
    """
    Render search results.

    Args:
        results: List of search results
        max_results: Maximum results to display
    """
    st.markdown("### Search Results")
    st.caption(f"Found {len(results)} result(s)")

    for result in results[:max_results]:
        with st.container():
            col1, col2, col3 = st.columns([1, 6, 2])

            with col1:
                st.markdown(f"<div style='font-size: 1.5rem; text-align: center;'>{result.icon}</div>", unsafe_allow_html=True)

            with col2:
                st.markdown(f"**{result.title}**")
                st.caption(result.subtitle)

            with col3:
                if st.button("Go →", key=f"search_go_{result.title}_{result.type}", use_container_width=True):
                    state = get_state()
                    state.navigate(result.page)

            st.markdown("---")


def render_command_palette():
    """Render command palette / spotlight search modal."""
    st.markdown("""
    <style>
    .command-palette-overlay {
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.5);
        z-index: 9999;
        display: none;
        align-items: flex-start;
        justify-content: center;
        padding-top: 100px;
    }

    .command-palette-overlay.open {
        display: flex;
    }

    .command-palette {
        background: var(--background);
        border-radius: var(--radius-xl);
        box-shadow: var(--shadow-xl);
        width: 600px;
        max-width: 90vw;
        max-height: 500px;
        overflow: hidden;
        animation: slideDown 0.2s ease-out;
    }

    @keyframes slideDown {
        from {
            opacity: 0;
            transform: translateY(-20px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    .command-palette-input {
        display: flex;
        align-items: center;
        padding: 1rem;
        border-bottom: 1px solid var(--border-light);
        gap: 0.75rem;
    }

    .command-palette-input input {
        flex: 1;
        border: none;
        outline: none;
        font-size: 1.125rem;
        background: transparent;
        color: var(--text-primary);
    }

    .command-palette-results {
        max-height: 400px;
        overflow-y: auto;
    }

    .command-palette-item {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 0.75rem 1rem;
        cursor: pointer;
        transition: background 0.15s ease;
    }

    .command-palette-item:hover,
    .command-palette-item.selected {
        background: var(--surface);
    }

    .command-palette-item-icon {
        font-size: 1.25rem;
    }

    .command-palette-item-content {
        flex: 1;
    }

    .command-palette-item-title {
        font-weight: 500;
    }

    .command-palette-item-subtitle {
        font-size: 0.75rem;
        color: var(--text-muted);
    }

    .command-palette-item-shortcut {
        display: flex;
        gap: 4px;
    }

    .command-palette-footer {
        padding: 0.5rem 1rem;
        border-top: 1px solid var(--border-light);
        display: flex;
        justify-content: space-between;
        font-size: 0.75rem;
        color: var(--text-muted);
    }
    </style>

    <div class="command-palette-overlay" id="commandPalette">
        <div class="command-palette">
            <div class="command-palette-input">
                <span>🔍</span>
                <input type="text" placeholder="Search or type a command..." id="commandPaletteInput" autocomplete="off">
                <span class="shortcut-key">ESC</span>
            </div>
            <div class="command-palette-results" id="commandPaletteResults">
                <!-- Results populated by JS -->
            </div>
            <div class="command-palette-footer">
                <span>↑↓ Navigate</span>
                <span>↵ Select</span>
                <span>ESC Close</span>
            </div>
        </div>
    </div>

    <script>
    (function() {
        const palette = document.getElementById('commandPalette');
        const input = document.getElementById('commandPaletteInput');
        const results = document.getElementById('commandPaletteResults');

        // Quick actions
        const actions = [
            {icon: '🏠', title: 'Go to Home', subtitle: 'Dashboard overview', action: 'home', shortcut: 'Ctrl+H'},
            {icon: '📥', title: 'Go to Data Ingestion', subtitle: 'Upload and ingest data', action: 'ingestion', shortcut: 'Ctrl+I'},
            {icon: '🔧', title: 'Go to Pipeline Builder', subtitle: 'Create ETL pipelines', action: 'pipelines', shortcut: 'Ctrl+P'},
            {icon: '📊', title: 'Go to Data Profiling', subtitle: 'Analyze data quality', action: 'profiling', shortcut: 'Ctrl+D'},
            {icon: '🌳', title: 'Go to Data Lineage', subtitle: 'View data flow', action: 'lineage', shortcut: 'Ctrl+L'},
            {icon: '💬', title: 'Go to Query Studio', subtitle: 'Natural language queries', action: 'query', shortcut: 'Ctrl+Q'},
            {icon: '📈', title: 'Go to Monitoring', subtitle: 'Pipeline monitoring', action: 'monitoring', shortcut: 'Ctrl+M'},
            {icon: '🔌', title: 'Go to Connectors', subtitle: 'Manage connections', action: 'connectors'},
            {icon: '⚙️', title: 'Go to Settings', subtitle: 'Configure settings', action: 'settings', shortcut: 'Ctrl+S'},
            {icon: '🔄', title: 'Refresh Data', subtitle: 'Reload current page', action: 'refresh', shortcut: 'Ctrl+R'},
            {icon: '🌙', title: 'Toggle Theme', subtitle: 'Switch light/dark mode', action: 'toggle_theme', shortcut: 'Ctrl+Shift+T'},
        ];

        function renderResults(query = '') {
            const filtered = query
                ? actions.filter(a => a.title.toLowerCase().includes(query.toLowerCase()) || a.subtitle.toLowerCase().includes(query.toLowerCase()))
                : actions;

            results.innerHTML = filtered.map((a, i) => `
                <div class="command-palette-item ${i === 0 ? 'selected' : ''}" data-action="${a.action}">
                    <span class="command-palette-item-icon">${a.icon}</span>
                    <div class="command-palette-item-content">
                        <div class="command-palette-item-title">${a.title}</div>
                        <div class="command-palette-item-subtitle">${a.subtitle}</div>
                    </div>
                    ${a.shortcut ? `<div class="command-palette-item-shortcut"><span class="shortcut-key">${a.shortcut}</span></div>` : ''}
                </div>
            `).join('');
        }

        function open() {
            palette.classList.add('open');
            input.value = '';
            renderResults();
            input.focus();
        }

        function close() {
            palette.classList.remove('open');
        }

        function execute(action) {
            close();
            sessionStorage.setItem('automic_shortcut_action', action);
            window.parent.postMessage({type: 'streamlit:rerun'}, '*');
        }

        // Event listeners
        input.addEventListener('input', (e) => renderResults(e.target.value));

        results.addEventListener('click', (e) => {
            const item = e.target.closest('.command-palette-item');
            if (item) execute(item.dataset.action);
        });

        document.addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
                e.preventDefault();
                palette.classList.contains('open') ? close() : open();
            }
            if (e.key === 'Escape') {
                close();
            }
        });

        palette.addEventListener('click', (e) => {
            if (e.target === palette) close();
        });

        // Make open function available globally
        window.openCommandPalette = open;
    })();
    </script>
    """, unsafe_allow_html=True)


def render_recent_searches():
    """Render recent search history."""
    state = get_state()
    history = state.get("search_history", [])

    if not history:
        return

    st.markdown("#### Recent Searches")

    for query in history[:5]:
        col1, col2 = st.columns([5, 1])
        with col1:
            if st.button(f"🕐 {query}", key=f"recent_{query}", use_container_width=True):
                search = get_global_search()
                results = search.search(query)
                state.set("search_results", results)
                state.set("global_search_query", query)
