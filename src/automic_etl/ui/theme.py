"""Minimal theme system for Automic ETL UI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from enum import Enum


class ThemeMode(str, Enum):
    """Theme modes."""
    LIGHT = "light"
    DARK = "dark"


@dataclass
class ColorPalette:
    """Minimal color palette - clean and professional."""
    primary: str = "#2563EB"
    primary_hover: str = "#1D4ED8"
    primary_light: str = "#EFF6FF"
    
    text_primary: str = "#111827"
    text_secondary: str = "#6B7280"
    text_muted: str = "#9CA3AF"
    text_inverse: str = "#FFFFFF"
    
    background: str = "#FFFFFF"
    surface: str = "#F9FAFB"
    border: str = "#E5E7EB"
    border_light: str = "#F3F4F6"
    
    success: str = "#059669"
    warning: str = "#D97706"
    danger: str = "#DC2626"
    
    bronze: str = "#A16207"
    silver: str = "#64748B"
    gold: str = "#CA8A04"


@dataclass
class DarkColorPalette(ColorPalette):
    """Dark theme palette."""
    primary: str = "#3B82F6"
    primary_hover: str = "#2563EB"
    primary_light: str = "#1E3A5F"
    
    text_primary: str = "#F9FAFB"
    text_secondary: str = "#9CA3AF"
    text_muted: str = "#6B7280"
    
    background: str = "#111827"
    surface: str = "#1F2937"
    border: str = "#374151"
    border_light: str = "#4B5563"


@dataclass
class Theme:
    """Theme configuration."""
    mode: ThemeMode = ThemeMode.LIGHT
    colors: ColorPalette = field(default_factory=ColorPalette)


LIGHT_THEME = Theme(mode=ThemeMode.LIGHT, colors=ColorPalette())
DARK_THEME = Theme(mode=ThemeMode.DARK, colors=DarkColorPalette())


def get_theme(mode: ThemeMode | str = ThemeMode.LIGHT) -> Theme:
    """Get theme by mode."""
    if isinstance(mode, str):
        mode = ThemeMode(mode)
    return DARK_THEME if mode == ThemeMode.DARK else LIGHT_THEME


def get_streamlit_css(theme: Theme | None = None) -> str:
    """Generate minimal Streamlit CSS."""
    theme = theme or LIGHT_THEME
    c = theme.colors
    
    return f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    :root {{
        --primary: {c.primary};
        --primary-hover: {c.primary_hover};
        --text-primary: {c.text_primary};
        --text-secondary: {c.text_secondary};
        --text-muted: {c.text_muted};
        --bg: {c.background};
        --surface: {c.surface};
        --border: {c.border};
        --success: {c.success};
        --warning: {c.warning};
        --danger: {c.danger};
    }}
    
    .stApp {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        background: var(--bg);
    }}
    
    /* Typography */
    h1, h2, h3, h4, h5, h6 {{
        font-family: 'Inter', sans-serif;
        color: var(--text-primary);
        font-weight: 600;
        letter-spacing: -0.025em;
    }}
    
    h1 {{ font-size: 1.875rem; font-weight: 700; }}
    h2 {{ font-size: 1.5rem; }}
    h3 {{ font-size: 1.25rem; }}
    
    p, span, div {{ color: var(--text-secondary); }}
    
    /* Sidebar - Clean dark design */
    [data-testid="stSidebar"] {{
        background: #111827;
        border-right: 1px solid #1F2937;
    }}
    
    [data-testid="stSidebar"] * {{
        color: #E5E7EB !important;
    }}
    
    [data-testid="stSidebar"] .stRadio > div {{
        gap: 2px;
    }}
    
    [data-testid="stSidebar"] .stRadio > div > label {{
        padding: 0.75rem 1rem;
        border-radius: 8px;
        font-size: 0.875rem;
        font-weight: 500;
        color: #9CA3AF !important;
        background: transparent;
        transition: all 0.15s ease;
        margin: 0;
    }}
    
    [data-testid="stSidebar"] .stRadio > div > label:hover {{
        background: rgba(255, 255, 255, 0.05);
        color: #F3F4F6 !important;
    }}
    
    [data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] {{
        background: var(--primary);
        color: white !important;
        font-weight: 600;
    }}
    
    [data-testid="stSidebar"] hr {{
        border-color: #374151;
        margin: 1rem 0;
    }}
    
    [data-testid="stSidebar"] .stTextInput input {{
        background: #1F2937;
        border: 1px solid #374151;
        color: #F3F4F6 !important;
        border-radius: 8px;
    }}
    
    [data-testid="stSidebar"] .stTextInput input:focus {{
        border-color: var(--primary);
    }}
    
    [data-testid="stSidebar"] .stTextInput input::placeholder {{
        color: #6B7280;
    }}
    
    /* Main content area */
    .main .block-container {{
        padding: 2rem 3rem;
        max-width: 1200px;
    }}
    
    /* Buttons */
    .stButton > button {{
        background: var(--surface);
        color: var(--text-primary);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        font-size: 0.875rem;
        transition: all 0.15s ease;
    }}
    
    .stButton > button:hover {{
        background: var(--border);
        border-color: var(--border);
    }}
    
    .stButton > button[kind="primary"],
    .stButton > button[data-testid="baseButton-primary"] {{
        background: var(--primary);
        color: white;
        border: none;
    }}
    
    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="baseButton-primary"]:hover {{
        background: var(--primary-hover);
    }}
    
    /* Inputs */
    .stTextInput > div > div > input,
    .stSelectbox > div > div,
    .stTextArea textarea {{
        background: var(--bg);
        border: 1px solid var(--border);
        border-radius: 8px;
        color: var(--text-primary);
        font-size: 0.875rem;
    }}
    
    .stTextInput > div > div > input:focus,
    .stTextArea textarea:focus {{
        border-color: var(--primary);
        box-shadow: 0 0 0 2px {c.primary_light};
    }}
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        background: var(--surface);
        padding: 4px;
        border-radius: 10px;
        border: 1px solid var(--border);
    }}
    
    .stTabs [data-baseweb="tab"] {{
        border-radius: 6px;
        padding: 0.5rem 1rem;
        color: var(--text-secondary);
        font-weight: 500;
        font-size: 0.875rem;
        background: transparent;
        border: none;
    }}
    
    .stTabs [aria-selected="true"] {{
        background: var(--bg);
        color: var(--text-primary);
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
    }}
    
    /* Metrics */
    [data-testid="stMetricLabel"] {{
        color: var(--text-muted);
        font-size: 0.75rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}
    
    [data-testid="stMetricValue"] {{
        color: var(--text-primary);
        font-size: 1.75rem;
        font-weight: 700;
        letter-spacing: -0.02em;
    }}
    
    /* Data tables */
    .stDataFrame {{
        border: 1px solid var(--border);
        border-radius: 10px;
        overflow: hidden;
    }}
    
    .stDataFrame thead th {{
        background: var(--surface);
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 0.75rem 1rem;
    }}
    
    .stDataFrame tbody tr:hover {{
        background: var(--surface);
    }}
    
    /* Expanders */
    .streamlit-expanderHeader {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 8px;
        font-weight: 500;
        padding: 0.75rem 1rem;
    }}
    
    .streamlit-expanderHeader:hover {{
        background: var(--border);
    }}
    
    /* Alerts */
    .stAlert {{
        border-radius: 8px;
        border: none;
    }}
    
    /* Progress bar */
    .stProgress > div > div {{
        background: var(--primary);
        border-radius: 999px;
    }}
    
    .stProgress > div {{
        background: var(--surface);
        border-radius: 999px;
    }}
    
    /* File uploader */
    [data-testid="stFileUploader"] {{
        border: 2px dashed var(--border);
        border-radius: 10px;
        padding: 2rem;
        background: var(--surface);
    }}
    
    [data-testid="stFileUploader"]:hover {{
        border-color: var(--primary);
        background: {c.primary_light};
    }}
    
    /* Code blocks */
    code {{
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        background: var(--surface);
        padding: 0.125rem 0.375rem;
        border-radius: 4px;
        font-size: 0.85em;
        color: var(--primary);
    }}
    
    pre {{
        background: #1F2937;
        color: #E5E7EB;
        padding: 1rem;
        border-radius: 8px;
        border: none;
    }}
    
    /* Scrollbar */
    ::-webkit-scrollbar {{
        width: 6px;
        height: 6px;
    }}
    
    ::-webkit-scrollbar-track {{
        background: transparent;
    }}
    
    ::-webkit-scrollbar-thumb {{
        background: var(--border);
        border-radius: 999px;
    }}
    
    ::-webkit-scrollbar-thumb:hover {{
        background: var(--text-muted);
    }}
    
    /* Utility classes */
    .card {{
        background: var(--bg);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1.5rem;
    }}
    
    .metric-card {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1.25rem;
    }}
    
    .status-success {{
        color: var(--success);
        background: #ECFDF5;
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
    }}
    
    .status-warning {{
        color: var(--warning);
        background: #FFFBEB;
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
    }}
    
    .status-danger {{
        color: var(--danger);
        background: #FEF2F2;
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
    }}
    </style>
    """
