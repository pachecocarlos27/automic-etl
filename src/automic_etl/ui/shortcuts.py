"""Keyboard shortcuts and accessibility features for Automic ETL UI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import streamlit as st


@dataclass
class Shortcut:
    """Keyboard shortcut definition."""
    key: str
    description: str
    action: str  # page name or action identifier
    modifier: str = ""  # ctrl, alt, shift, meta
    category: str = "General"


# Define all keyboard shortcuts
SHORTCUTS = [
    # Navigation shortcuts
    Shortcut("h", "Go to Home", "home", "ctrl", "Navigation"),
    Shortcut("i", "Go to Data Ingestion", "ingestion", "ctrl", "Navigation"),
    Shortcut("p", "Go to Pipeline Builder", "pipelines", "ctrl", "Navigation"),
    Shortcut("d", "Go to Data Profiling", "profiling", "ctrl", "Navigation"),
    Shortcut("l", "Go to Data Lineage", "lineage", "ctrl", "Navigation"),
    Shortcut("q", "Go to Query Studio", "query", "ctrl", "Navigation"),
    Shortcut("m", "Go to Monitoring", "monitoring", "ctrl", "Navigation"),
    Shortcut("s", "Go to Settings", "settings", "ctrl", "Navigation"),

    # Action shortcuts
    Shortcut("k", "Open Command Palette / Search", "search", "ctrl", "Actions"),
    Shortcut("/", "Focus Search", "focus_search", "", "Actions"),
    Shortcut("n", "New Pipeline", "new_pipeline", "ctrl+shift", "Actions"),
    Shortcut("r", "Refresh Data", "refresh", "ctrl", "Actions"),
    Shortcut("e", "Export Data", "export", "ctrl+shift", "Actions"),

    # UI shortcuts
    Shortcut("b", "Toggle Sidebar", "toggle_sidebar", "ctrl", "UI"),
    Shortcut("t", "Toggle Theme", "toggle_theme", "ctrl+shift", "UI"),
    Shortcut("?", "Show Help / Shortcuts", "help", "ctrl", "UI"),
    Shortcut("Escape", "Close Modal / Cancel", "close", "", "UI"),

    # Data shortcuts
    Shortcut("1", "Select Bronze Layer", "select_bronze", "alt", "Data"),
    Shortcut("2", "Select Silver Layer", "select_silver", "alt", "Data"),
    Shortcut("3", "Select Gold Layer", "select_gold", "alt", "Data"),
]


def get_shortcuts_by_category() -> dict[str, list[Shortcut]]:
    """Get shortcuts organized by category."""
    by_category: dict[str, list[Shortcut]] = {}
    for shortcut in SHORTCUTS:
        if shortcut.category not in by_category:
            by_category[shortcut.category] = []
        by_category[shortcut.category].append(shortcut)
    return by_category


def get_shortcut_display(shortcut: Shortcut) -> str:
    """Get display string for a shortcut."""
    parts = []
    if shortcut.modifier:
        for mod in shortcut.modifier.split("+"):
            if mod == "ctrl":
                parts.append("Ctrl")
            elif mod == "alt":
                parts.append("Alt")
            elif mod == "shift":
                parts.append("Shift")
            elif mod == "meta":
                parts.append("Cmd")

    parts.append(shortcut.key.upper() if len(shortcut.key) == 1 else shortcut.key)
    return " + ".join(parts)


def inject_keyboard_shortcuts():
    """Inject keyboard shortcut handler JavaScript."""
    shortcuts_js = """
    <script>
    (function() {
        // Prevent duplicate initialization
        if (window.automicShortcutsInitialized) return;
        window.automicShortcutsInitialized = true;

        const shortcuts = {
            'ctrl+h': 'home',
            'ctrl+i': 'ingestion',
            'ctrl+p': 'pipelines',
            'ctrl+d': 'profiling',
            'ctrl+l': 'lineage',
            'ctrl+q': 'query',
            'ctrl+m': 'monitoring',
            'ctrl+s': 'settings',
            'ctrl+k': 'search',
            'ctrl+b': 'toggle_sidebar',
            'ctrl+shift+t': 'toggle_theme',
            'ctrl+r': 'refresh',
            'ctrl+shift+e': 'export',
            'ctrl+shift+n': 'new_pipeline',
            'ctrl+?': 'help',
            'alt+1': 'select_bronze',
            'alt+2': 'select_silver',
            'alt+3': 'select_gold',
        };

        document.addEventListener('keydown', function(e) {
            // Don't trigger if typing in input
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
                // Allow Escape to blur input
                if (e.key === 'Escape') {
                    e.target.blur();
                }
                return;
            }

            let key = [];
            if (e.ctrlKey || e.metaKey) key.push('ctrl');
            if (e.altKey) key.push('alt');
            if (e.shiftKey) key.push('shift');
            key.push(e.key.toLowerCase());

            const combo = key.join('+');
            const action = shortcuts[combo];

            if (action) {
                e.preventDefault();

                // Handle special actions
                if (action === 'search') {
                    // Focus search or open search modal
                    const searchInput = document.querySelector('[data-testid="stTextInput"] input');
                    if (searchInput) {
                        searchInput.focus();
                    }
                } else if (action === 'toggle_sidebar') {
                    // Toggle sidebar
                    const sidebarButton = document.querySelector('[data-testid="collapsedControl"]');
                    if (sidebarButton) sidebarButton.click();
                } else if (action === 'refresh') {
                    // Trigger Streamlit rerun
                    window.parent.postMessage({type: 'streamlit:rerun'}, '*');
                } else {
                    // Navigation actions - store in sessionStorage for Streamlit to read
                    sessionStorage.setItem('automic_shortcut_action', action);
                    window.parent.postMessage({type: 'streamlit:rerun'}, '*');
                }
            }

            // Handle "/" for quick search focus
            if (e.key === '/' && !e.ctrlKey && !e.altKey && !e.shiftKey) {
                e.preventDefault();
                const searchInput = document.querySelector('[data-testid="stTextInput"] input');
                if (searchInput) {
                    searchInput.focus();
                }
            }

            // Handle Escape
            if (e.key === 'Escape') {
                // Close any open expanders or modals
                const closeButtons = document.querySelectorAll('[aria-label="Close"]');
                closeButtons.forEach(btn => btn.click());
            }
        });

        // Add visual indicator for keyboard navigation
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Tab') {
                document.body.classList.add('keyboard-navigation');
            }
        });

        document.addEventListener('mousedown', function() {
            document.body.classList.remove('keyboard-navigation');
        });
    })();
    </script>

    <style>
    /* Keyboard navigation focus styles */
    body.keyboard-navigation *:focus {
        outline: 2px solid var(--primary) !important;
        outline-offset: 2px !important;
    }

    /* Skip to main content link */
    .skip-link {
        position: absolute;
        top: -40px;
        left: 0;
        background: var(--primary);
        color: white;
        padding: 8px;
        z-index: 100;
        transition: top 0.3s;
    }

    .skip-link:focus {
        top: 0;
    }

    /* Shortcut hints */
    .shortcut-hint {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-left: 8px;
    }

    .shortcut-key {
        display: inline-block;
        padding: 2px 6px;
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 4px;
        font-family: var(--font-family-mono);
        font-size: 0.7rem;
        min-width: 20px;
        text-align: center;
    }
    </style>
    """

    st.markdown(shortcuts_js, unsafe_allow_html=True)


def show_shortcuts_modal():
    """Display keyboard shortcuts help modal."""
    st.markdown("### Keyboard Shortcuts")

    shortcuts_by_cat = get_shortcuts_by_category()

    for category, shortcuts in shortcuts_by_cat.items():
        st.markdown(f"#### {category}")

        for shortcut in shortcuts:
            col1, col2 = st.columns([1, 3])
            with col1:
                keys = get_shortcut_display(shortcut)
                st.markdown(f"""
                <div style="display: flex; gap: 4px;">
                    {"".join(f'<span class="shortcut-key">{k}</span>' for k in keys.split(" + "))}
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(shortcut.description)

        st.markdown("---")


def shortcut_hint(shortcut_key: str) -> str:
    """Generate HTML for a shortcut hint."""
    shortcut = next((s for s in SHORTCUTS if s.action == shortcut_key), None)
    if not shortcut:
        return ""

    keys = get_shortcut_display(shortcut)
    key_html = "".join(f'<span class="shortcut-key">{k}</span>' for k in keys.split(" + "))

    return f'<span class="shortcut-hint">{key_html}</span>'


def render_skip_link():
    """Render skip to main content link for accessibility."""
    st.markdown("""
    <a href="#main-content" class="skip-link">Skip to main content</a>
    """, unsafe_allow_html=True)


def add_aria_labels():
    """Add ARIA labels to common elements for screen readers."""
    st.markdown("""
    <script>
    (function() {
        // Add ARIA labels to navigation
        const nav = document.querySelector('[data-testid="stSidebar"]');
        if (nav) {
            nav.setAttribute('role', 'navigation');
            nav.setAttribute('aria-label', 'Main navigation');
        }

        // Add ARIA labels to main content
        const main = document.querySelector('.main');
        if (main) {
            main.setAttribute('role', 'main');
            main.setAttribute('id', 'main-content');
            main.setAttribute('aria-label', 'Main content');
        }

        // Add ARIA labels to buttons without text
        document.querySelectorAll('button').forEach(btn => {
            if (!btn.textContent.trim() && !btn.getAttribute('aria-label')) {
                const icon = btn.querySelector('svg, img');
                if (icon) {
                    btn.setAttribute('aria-label', icon.getAttribute('title') || 'Button');
                }
            }
        });

        // Add ARIA live region for notifications
        if (!document.getElementById('aria-live-region')) {
            const liveRegion = document.createElement('div');
            liveRegion.id = 'aria-live-region';
            liveRegion.setAttribute('role', 'status');
            liveRegion.setAttribute('aria-live', 'polite');
            liveRegion.setAttribute('aria-atomic', 'true');
            liveRegion.style.cssText = 'position: absolute; left: -9999px;';
            document.body.appendChild(liveRegion);
        }
    })();
    </script>
    """, unsafe_allow_html=True)


def announce_to_screen_reader(message: str):
    """Announce a message to screen readers."""
    st.markdown(f"""
    <script>
    (function() {{
        const liveRegion = document.getElementById('aria-live-region');
        if (liveRegion) {{
            liveRegion.textContent = '{message}';
        }}
    }})();
    </script>
    """, unsafe_allow_html=True)
