"""Sleek, minimal design system for Automic ETL UI."""

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
    """Modern, minimal color palette with refined aesthetics."""

    # Primary - Deep slate blue (sophisticated, professional)
    primary: str = "#0F172A"
    primary_hover: str = "#1E293B"
    primary_muted: str = "#334155"
    on_primary: str = "#FFFFFF"

    # Accent - Vibrant indigo (for CTAs and highlights)
    accent: str = "#6366F1"
    accent_hover: str = "#4F46E5"
    accent_light: str = "#EEF2FF"
    on_accent: str = "#FFFFFF"

    # Success
    success: str = "#10B981"
    success_light: str = "#D1FAE5"
    success_dark: str = "#059669"

    # Warning
    warning: str = "#F59E0B"
    warning_light: str = "#FEF3C7"
    warning_dark: str = "#D97706"

    # Error
    error: str = "#EF4444"
    error_light: str = "#FEE2E2"
    error_dark: str = "#DC2626"

    # Info
    info: str = "#3B82F6"
    info_light: str = "#DBEAFE"
    info_dark: str = "#2563EB"

    # Neutrals - Refined grayscale
    white: str = "#FFFFFF"
    gray_50: str = "#F8FAFC"
    gray_100: str = "#F1F5F9"
    gray_200: str = "#E2E8F0"
    gray_300: str = "#CBD5E1"
    gray_400: str = "#94A3B8"
    gray_500: str = "#64748B"
    gray_600: str = "#475569"
    gray_700: str = "#334155"
    gray_800: str = "#1E293B"
    gray_900: str = "#0F172A"
    black: str = "#020617"

    # Surface colors
    background: str = "#FAFBFC"
    surface: str = "#FFFFFF"
    surface_elevated: str = "#FFFFFF"
    surface_muted: str = "#F8FAFC"

    # Text colors
    text_primary: str = "#0F172A"
    text_secondary: str = "#475569"
    text_muted: str = "#94A3B8"
    text_inverse: str = "#FFFFFF"

    # Border colors
    border: str = "#E2E8F0"
    border_hover: str = "#CBD5E1"
    border_focus: str = "#6366F1"

    # Medallion architecture colors (refined)
    bronze: str = "#A16207"
    bronze_bg: str = "#FEF9C3"
    silver: str = "#6B7280"
    silver_bg: str = "#F3F4F6"
    gold: str = "#CA8A04"
    gold_bg: str = "#FEF3C7"


@dataclass
class DarkColorPalette(ColorPalette):
    """Dark theme color palette."""

    # Primary inverted for dark mode
    primary: str = "#F8FAFC"
    primary_hover: str = "#E2E8F0"
    primary_muted: str = "#CBD5E1"
    on_primary: str = "#0F172A"

    # Accent stays vibrant
    accent: str = "#818CF8"
    accent_hover: str = "#6366F1"
    accent_light: str = "#1E1B4B"

    # Status colors (slightly muted for dark)
    success: str = "#34D399"
    success_light: str = "#064E3B"
    success_dark: str = "#10B981"

    warning: str = "#FBBF24"
    warning_light: str = "#78350F"
    warning_dark: str = "#F59E0B"

    error: str = "#F87171"
    error_light: str = "#7F1D1D"
    error_dark: str = "#EF4444"

    info: str = "#60A5FA"
    info_light: str = "#1E3A5F"
    info_dark: str = "#3B82F6"

    # Surface colors (dark)
    background: str = "#0F172A"
    surface: str = "#1E293B"
    surface_elevated: str = "#334155"
    surface_muted: str = "#1E293B"

    # Text colors (inverted)
    text_primary: str = "#F8FAFC"
    text_secondary: str = "#CBD5E1"
    text_muted: str = "#64748B"
    text_inverse: str = "#0F172A"

    # Border colors (dark)
    border: str = "#334155"
    border_hover: str = "#475569"
    border_focus: str = "#818CF8"

    # Medallion (dark mode)
    bronze: str = "#FCD34D"
    bronze_bg: str = "#422006"
    silver: str = "#D1D5DB"
    silver_bg: str = "#374151"
    gold: str = "#FBBF24"
    gold_bg: str = "#78350F"


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
    """Generate sleek, minimal Streamlit CSS."""
    theme = theme or LIGHT_THEME
    c = theme.colors
    is_dark = theme.mode == ThemeMode.DARK

    return f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap');

    :root {{
        /* Core colors */
        --primary: {c.primary};
        --primary-hover: {c.primary_hover};
        --primary-muted: {c.primary_muted};
        --on-primary: {c.on_primary};

        --accent: {c.accent};
        --accent-hover: {c.accent_hover};
        --accent-light: {c.accent_light};
        --on-accent: {c.on_accent};

        /* Status colors */
        --success: {c.success};
        --success-light: {c.success_light};
        --warning: {c.warning};
        --warning-light: {c.warning_light};
        --error: {c.error};
        --error-light: {c.error_light};
        --info: {c.info};
        --info-light: {c.info_light};

        /* Surfaces */
        --bg: {c.background};
        --surface: {c.surface};
        --surface-elevated: {c.surface_elevated};
        --surface-muted: {c.surface_muted};

        /* Text */
        --text: {c.text_primary};
        --text-secondary: {c.text_secondary};
        --text-muted: {c.text_muted};
        --text-inverse: {c.text_inverse};

        /* Borders */
        --border: {c.border};
        --border-hover: {c.border_hover};
        --border-focus: {c.border_focus};

        /* Medallion */
        --bronze: {c.bronze};
        --bronze-bg: {c.bronze_bg};
        --silver: {c.silver};
        --silver-bg: {c.silver_bg};
        --gold: {c.gold};
        --gold-bg: {c.gold_bg};

        /* Spacing scale */
        --space-1: 0.25rem;
        --space-2: 0.5rem;
        --space-3: 0.75rem;
        --space-4: 1rem;
        --space-5: 1.25rem;
        --space-6: 1.5rem;
        --space-8: 2rem;
        --space-10: 2.5rem;
        --space-12: 3rem;

        /* Border radius */
        --radius-sm: 6px;
        --radius-md: 8px;
        --radius-lg: 12px;
        --radius-xl: 16px;
        --radius-full: 9999px;

        /* Shadows */
        --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
        --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
        --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
        --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.1), 0 8px 10px -6px rgb(0 0 0 / 0.1);

        /* Transitions */
        --transition-fast: 150ms cubic-bezier(0.4, 0, 0.2, 1);
        --transition-base: 200ms cubic-bezier(0.4, 0, 0.2, 1);
        --transition-slow: 300ms cubic-bezier(0.4, 0, 0.2, 1);
    }}

    /* ========================================
       BASE STYLES
       ======================================== */

    .stApp {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background: var(--bg);
        color: var(--text);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Typography */
    h1, h2, h3, h4, h5, h6 {{
        font-family: 'Inter', sans-serif;
        color: var(--text);
        font-weight: 600;
        letter-spacing: -0.02em;
        line-height: 1.2;
        margin: 0;
    }}

    h1 {{ font-size: 1.875rem; font-weight: 700; letter-spacing: -0.025em; }}
    h2 {{ font-size: 1.5rem; font-weight: 600; }}
    h3 {{ font-size: 1.25rem; font-weight: 600; }}
    h4 {{ font-size: 1.125rem; font-weight: 500; }}
    h5 {{ font-size: 1rem; font-weight: 500; }}

    p, span, div, label {{
        font-size: 0.875rem;
        line-height: 1.6;
        color: var(--text-secondary);
    }}

    /* ========================================
       SIDEBAR - Sleek dark sidebar
       ======================================== */

    [data-testid="stSidebar"] {{
        background: {c.gray_900};
        border-right: 1px solid {c.gray_800};
    }}

    [data-testid="stSidebar"] > div:first-child {{
        padding: var(--space-4);
    }}

    [data-testid="stSidebar"] * {{
        color: {c.gray_300} !important;
    }}

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {{
        color: {c.white} !important;
        font-weight: 600;
    }}

    /* Sidebar navigation items */
    [data-testid="stSidebar"] .stRadio > div {{
        gap: 2px;
    }}

    [data-testid="stSidebar"] .stRadio > div > label {{
        padding: 10px 12px;
        border-radius: var(--radius-md);
        font-size: 0.875rem;
        font-weight: 450;
        color: {c.gray_400} !important;
        background: transparent;
        transition: var(--transition-fast);
        margin: 0;
        cursor: pointer;
    }}

    [data-testid="stSidebar"] .stRadio > div > label:hover {{
        background: rgba(255, 255, 255, 0.06);
        color: {c.white} !important;
    }}

    [data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] {{
        background: {c.accent};
        color: {c.white} !important;
        font-weight: 500;
    }}

    /* Sidebar search input */
    [data-testid="stSidebar"] .stTextInput input {{
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid {c.gray_700};
        color: {c.white} !important;
        border-radius: var(--radius-md);
        font-size: 0.875rem;
        padding: 10px 12px;
        transition: var(--transition-fast);
    }}

    [data-testid="stSidebar"] .stTextInput input:focus {{
        border-color: {c.accent};
        background: rgba(255, 255, 255, 0.08);
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.15);
    }}

    [data-testid="stSidebar"] .stTextInput input::placeholder {{
        color: {c.gray_500};
    }}

    [data-testid="stSidebar"] hr {{
        border-color: {c.gray_800};
        margin: var(--space-4) 0;
    }}

    /* ========================================
       MAIN CONTENT
       ======================================== */

    .main .block-container {{
        padding: var(--space-8) var(--space-10);
        max-width: 1400px;
    }}

    /* ========================================
       BUTTONS - Minimal, refined
       ======================================== */

    .stButton > button {{
        background: var(--surface);
        color: var(--text);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 10px 16px;
        font-weight: 500;
        font-size: 0.875rem;
        transition: var(--transition-fast);
        box-shadow: none;
        line-height: 1;
    }}

    .stButton > button:hover {{
        background: var(--surface-muted);
        border-color: var(--border-hover);
    }}

    .stButton > button:active {{
        transform: scale(0.98);
    }}

    /* Primary button */
    .stButton > button[kind="primary"],
    .stButton > button[data-testid="baseButton-primary"] {{
        background: var(--accent);
        color: var(--on-accent);
        border: none;
        box-shadow: var(--shadow-sm);
    }}

    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="baseButton-primary"]:hover {{
        background: var(--accent-hover);
        box-shadow: var(--shadow-md);
    }}

    /* Secondary button */
    .stButton > button[kind="secondary"],
    .stButton > button[data-testid="baseButton-secondary"] {{
        background: var(--surface-muted);
        color: var(--text-secondary);
        border: 1px solid var(--border);
    }}

    .stButton > button[kind="secondary"]:hover,
    .stButton > button[data-testid="baseButton-secondary"]:hover {{
        background: var(--border);
        color: var(--text);
    }}

    /* ========================================
       INPUTS - Clean, minimal
       ======================================== */

    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stTextArea textarea {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        color: var(--text);
        font-size: 0.875rem;
        padding: 10px 12px;
        transition: var(--transition-fast);
    }}

    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus,
    .stTextArea textarea:focus {{
        border-color: var(--accent);
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
        outline: none;
    }}

    .stTextInput > label,
    .stTextArea > label,
    .stSelectbox > label,
    .stNumberInput > label {{
        color: var(--text-secondary);
        font-size: 0.875rem;
        font-weight: 500;
        margin-bottom: var(--space-1);
    }}

    /* Select boxes */
    .stSelectbox > div > div {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
    }}

    .stSelectbox > div > div:hover {{
        border-color: var(--border-hover);
    }}

    /* Multiselect */
    .stMultiSelect > div > div {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
    }}

    /* ========================================
       TABS - Segmented control style
       ======================================== */

    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        background: var(--surface-muted);
        padding: 4px;
        border-radius: var(--radius-lg);
        border: 1px solid var(--border);
    }}

    .stTabs [data-baseweb="tab"] {{
        border-radius: var(--radius-md);
        padding: 8px 16px;
        color: var(--text-muted);
        font-weight: 500;
        font-size: 0.875rem;
        background: transparent;
        border: none;
        transition: var(--transition-fast);
    }}

    .stTabs [data-baseweb="tab"]:hover {{
        color: var(--text-secondary);
        background: var(--surface);
    }}

    .stTabs [aria-selected="true"] {{
        background: var(--surface);
        color: var(--text);
        box-shadow: var(--shadow-sm);
    }}

    .stTabs [data-baseweb="tab-highlight"] {{
        display: none;
    }}

    .stTabs [data-baseweb="tab-border"] {{
        display: none;
    }}

    /* ========================================
       METRICS - Clean display
       ======================================== */

    [data-testid="stMetricLabel"] {{
        color: var(--text-muted);
        font-size: 0.75rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    [data-testid="stMetricValue"] {{
        color: var(--text);
        font-size: 2rem;
        font-weight: 700;
        letter-spacing: -0.02em;
    }}

    [data-testid="stMetricDelta"] {{
        font-size: 0.75rem;
        font-weight: 500;
    }}

    [data-testid="stMetricDelta"] svg {{
        width: 12px;
        height: 12px;
    }}

    /* ========================================
       DATA TABLES - Minimal design
       ======================================== */

    .stDataFrame {{
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        overflow: hidden;
    }}

    .stDataFrame thead th {{
        background: var(--surface-muted);
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        padding: 12px 16px;
        color: var(--text-secondary);
        border-bottom: 1px solid var(--border);
    }}

    .stDataFrame tbody td {{
        padding: 12px 16px;
        font-size: 0.875rem;
        border-bottom: 1px solid var(--border);
    }}

    .stDataFrame tbody tr:last-child td {{
        border-bottom: none;
    }}

    .stDataFrame tbody tr:hover {{
        background: var(--surface-muted);
    }}

    /* ========================================
       EXPANDERS - Clean accordion
       ======================================== */

    .streamlit-expanderHeader {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        font-weight: 500;
        font-size: 0.875rem;
        padding: 12px 16px;
        transition: var(--transition-fast);
    }}

    .streamlit-expanderHeader:hover {{
        background: var(--surface-muted);
        border-color: var(--border-hover);
    }}

    .streamlit-expanderContent {{
        border: 1px solid var(--border);
        border-top: none;
        border-radius: 0 0 var(--radius-md) var(--radius-md);
        padding: var(--space-4);
    }}

    /* ========================================
       ALERTS - Subtle, informative
       ======================================== */

    .stAlert {{
        border-radius: var(--radius-md);
        border: none;
        padding: 12px 16px;
        font-size: 0.875rem;
    }}

    div[data-testid="stAlert"][data-baseweb="notification"] {{
        background: var(--info-light);
        color: var(--info);
    }}

    /* ========================================
       PROGRESS - Minimal bar
       ======================================== */

    .stProgress > div {{
        background: var(--surface-muted);
        border-radius: var(--radius-full);
        height: 6px;
    }}

    .stProgress > div > div {{
        background: var(--accent);
        border-radius: var(--radius-full);
    }}

    /* ========================================
       FILE UPLOADER - Clean dropzone
       ======================================== */

    [data-testid="stFileUploader"] {{
        border: 2px dashed var(--border);
        border-radius: var(--radius-lg);
        padding: var(--space-8);
        background: var(--surface-muted);
        transition: var(--transition-fast);
    }}

    [data-testid="stFileUploader"]:hover {{
        border-color: var(--accent);
        background: var(--accent-light);
    }}

    /* ========================================
       CODE BLOCKS
       ======================================== */

    code {{
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        background: var(--surface-muted);
        padding: 2px 6px;
        border-radius: var(--radius-sm);
        font-size: 0.85em;
        color: var(--accent);
    }}

    pre {{
        background: {c.gray_900};
        color: {c.gray_100};
        padding: var(--space-4);
        border-radius: var(--radius-md);
        border: 1px solid {c.gray_800};
        font-size: 0.85rem;
        line-height: 1.6;
    }}

    /* ========================================
       CHECKBOXES & RADIOS
       ======================================== */

    .stCheckbox > label,
    .stRadio > label {{
        color: var(--text-secondary);
        font-size: 0.875rem;
    }}

    .stCheckbox > label > span:first-child,
    .stRadio > label > span:first-child {{
        border-color: var(--border);
    }}

    /* ========================================
       DIVIDERS
       ======================================== */

    hr {{
        border: none;
        border-top: 1px solid var(--border);
        margin: var(--space-6) 0;
    }}

    /* ========================================
       SCROLLBAR - Minimal
       ======================================== */

    ::-webkit-scrollbar {{
        width: 6px;
        height: 6px;
    }}

    ::-webkit-scrollbar-track {{
        background: transparent;
    }}

    ::-webkit-scrollbar-thumb {{
        background: var(--border);
        border-radius: var(--radius-full);
    }}

    ::-webkit-scrollbar-thumb:hover {{
        background: var(--text-muted);
    }}

    /* ========================================
       CUSTOM COMPONENTS
       ======================================== */

    /* Card component */
    .card {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: var(--space-6);
    }}

    .card-elevated {{
        background: var(--surface);
        border: none;
        border-radius: var(--radius-lg);
        padding: var(--space-6);
        box-shadow: var(--shadow-md);
    }}

    .card-hover {{
        transition: var(--transition-fast);
    }}

    .card-hover:hover {{
        border-color: var(--border-hover);
        box-shadow: var(--shadow-sm);
    }}

    /* Metric card */
    .metric-card {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: var(--space-5);
        transition: var(--transition-fast);
    }}

    .metric-card:hover {{
        border-color: var(--border-hover);
    }}

    /* Status badges */
    .badge {{
        display: inline-flex;
        align-items: center;
        padding: 4px 10px;
        border-radius: var(--radius-full);
        font-size: 0.75rem;
        font-weight: 500;
        letter-spacing: 0.01em;
    }}

    .badge-success {{
        color: var(--success);
        background: var(--success-light);
    }}

    .badge-warning {{
        color: var(--warning);
        background: var(--warning-light);
    }}

    .badge-error {{
        color: var(--error);
        background: var(--error-light);
    }}

    .badge-info {{
        color: var(--info);
        background: var(--info-light);
    }}

    .badge-neutral {{
        color: var(--text-secondary);
        background: var(--surface-muted);
    }}

    /* Medallion tier badges */
    .tier-bronze {{
        color: var(--bronze);
        background: var(--bronze-bg);
    }}

    .tier-silver {{
        color: var(--silver);
        background: var(--silver-bg);
    }}

    .tier-gold {{
        color: var(--gold);
        background: var(--gold-bg);
    }}

    /* Empty state */
    .empty-state {{
        text-align: center;
        padding: var(--space-12) var(--space-8);
        color: var(--text-muted);
    }}

    .empty-state-icon {{
        font-size: 3rem;
        margin-bottom: var(--space-4);
        opacity: 0.5;
    }}

    /* Page header */
    .page-header {{
        margin-bottom: var(--space-8);
    }}

    .page-header h1 {{
        margin-bottom: var(--space-2);
    }}

    .page-header p {{
        color: var(--text-muted);
        font-size: 1rem;
        margin: 0;
    }}

    /* Stat grid */
    .stat-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: var(--space-4);
    }}

    /* Action bar */
    .action-bar {{
        display: flex;
        align-items: center;
        gap: var(--space-3);
        padding: var(--space-4) 0;
        border-bottom: 1px solid var(--border);
        margin-bottom: var(--space-6);
    }}

    /* Focus states */
    *:focus-visible {{
        outline: 2px solid var(--accent);
        outline-offset: 2px;
    }}

    /* Animations */
    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(4px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}

    .animate-in {{
        animation: fadeIn 0.2s ease-out;
    }}

    /* Loading skeleton */
    .skeleton {{
        background: linear-gradient(90deg, var(--surface-muted) 25%, var(--border) 50%, var(--surface-muted) 75%);
        background-size: 200% 100%;
        animation: shimmer 1.5s infinite;
        border-radius: var(--radius-sm);
    }}

    @keyframes shimmer {{
        0% {{ background-position: 200% 0; }}
        100% {{ background-position: -200% 0; }}
    }}

    /* Hide Streamlit branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
    </style>
    """
