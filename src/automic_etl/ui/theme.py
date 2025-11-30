"""Theme system for Automic ETL UI - Professional Data Platform Design."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from enum import Enum


class ThemeMode(str, Enum):
    """Theme modes."""
    LIGHT = "light"
    DARK = "dark"
    SYSTEM = "system"


@dataclass
class ColorPalette:
    """Color palette for a theme - Modern Professional Design."""
    primary: str = "#0066FF"
    primary_hover: str = "#0052CC"
    primary_light: str = "#E6F0FF"

    secondary: str = "#6B7280"
    secondary_hover: str = "#4B5563"

    accent: str = "#00D4AA"
    accent_hover: str = "#00B894"

    success: str = "#10B981"
    success_light: str = "#D1FAE5"
    warning: str = "#F59E0B"
    warning_light: str = "#FEF3C7"
    danger: str = "#EF4444"
    danger_light: str = "#FEE2E2"
    info: str = "#3B82F6"
    info_light: str = "#DBEAFE"

    background: str = "#FFFFFF"
    surface: str = "#F8FAFC"
    surface_hover: str = "#F1F5F9"
    border: str = "#E2E8F0"
    border_light: str = "#F1F5F9"

    text_primary: str = "#0F172A"
    text_secondary: str = "#64748B"
    text_muted: str = "#94A3B8"
    text_inverse: str = "#FFFFFF"

    bronze: str = "#B45309"
    silver: str = "#6B7280"
    gold: str = "#D97706"

    gradient_start: str = "#0066FF"
    gradient_end: str = "#00D4AA"


@dataclass
class DarkColorPalette(ColorPalette):
    """Dark theme color palette - Professional Dark Mode."""
    primary: str = "#3B82F6"
    primary_hover: str = "#2563EB"
    primary_light: str = "#1E3A5F"

    background: str = "#0F172A"
    surface: str = "#1E293B"
    surface_hover: str = "#334155"
    border: str = "#334155"
    border_light: str = "#475569"

    text_primary: str = "#F8FAFC"
    text_secondary: str = "#94A3B8"
    text_muted: str = "#64748B"

    success_light: str = "#064E3B"
    warning_light: str = "#78350F"
    danger_light: str = "#7F1D1D"
    info_light: str = "#1E3A8A"

    gradient_start: str = "#3B82F6"
    gradient_end: str = "#06B6D4"


@dataclass
class Typography:
    """Typography settings - Clean Modern Fonts."""
    font_family: str = "'Inter', 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"
    font_family_mono: str = "'JetBrains Mono', 'SF Mono', 'Fira Code', 'Consolas', monospace"

    font_size_xs: str = "0.75rem"
    font_size_sm: str = "0.875rem"
    font_size_base: str = "1rem"
    font_size_lg: str = "1.125rem"
    font_size_xl: str = "1.25rem"
    font_size_2xl: str = "1.5rem"
    font_size_3xl: str = "2rem"
    font_size_4xl: str = "2.5rem"

    font_weight_light: int = 300
    font_weight_normal: int = 400
    font_weight_medium: int = 500
    font_weight_semibold: int = 600
    font_weight_bold: int = 700

    line_height_tight: float = 1.25
    line_height_normal: float = 1.5
    line_height_relaxed: float = 1.75


@dataclass
class Spacing:
    """Spacing scale."""
    xs: str = "0.25rem"
    sm: str = "0.5rem"
    md: str = "1rem"
    lg: str = "1.5rem"
    xl: str = "2rem"
    xxl: str = "3rem"


@dataclass
class Shadows:
    """Shadow definitions - Subtle Professional Shadows."""
    sm: str = "0 1px 2px 0 rgba(0, 0, 0, 0.03)"
    md: str = "0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03)"
    lg: str = "0 10px 15px -3px rgba(0, 0, 0, 0.08), 0 4px 6px -2px rgba(0, 0, 0, 0.04)"
    xl: str = "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)"
    glow: str = "0 0 20px rgba(0, 102, 255, 0.15)"


@dataclass
class BorderRadius:
    """Border radius scale."""
    none: str = "0"
    sm: str = "0.375rem"
    md: str = "0.5rem"
    lg: str = "0.75rem"
    xl: str = "1rem"
    xxl: str = "1.5rem"
    full: str = "9999px"


@dataclass
class Theme:
    """Complete theme configuration."""
    name: str = "default"
    mode: ThemeMode = ThemeMode.LIGHT
    colors: ColorPalette = field(default_factory=ColorPalette)
    typography: Typography = field(default_factory=Typography)
    spacing: Spacing = field(default_factory=Spacing)
    shadows: Shadows = field(default_factory=Shadows)
    border_radius: BorderRadius = field(default_factory=BorderRadius)

    def get_css_variables(self) -> str:
        """Generate CSS custom properties from theme."""
        return f"""
        :root {{
            --primary: {self.colors.primary};
            --primary-hover: {self.colors.primary_hover};
            --primary-light: {self.colors.primary_light};

            --secondary: {self.colors.secondary};
            --secondary-hover: {self.colors.secondary_hover};

            --accent: {self.colors.accent};
            --accent-hover: {self.colors.accent_hover};

            --success: {self.colors.success};
            --success-light: {self.colors.success_light};
            --warning: {self.colors.warning};
            --warning-light: {self.colors.warning_light};
            --danger: {self.colors.danger};
            --danger-light: {self.colors.danger_light};
            --info: {self.colors.info};
            --info-light: {self.colors.info_light};

            --background: {self.colors.background};
            --surface: {self.colors.surface};
            --surface-hover: {self.colors.surface_hover};
            --border: {self.colors.border};
            --border-light: {self.colors.border_light};

            --text-primary: {self.colors.text_primary};
            --text-secondary: {self.colors.text_secondary};
            --text-muted: {self.colors.text_muted};
            --text-inverse: {self.colors.text_inverse};

            --bronze: {self.colors.bronze};
            --silver: {self.colors.silver};
            --gold: {self.colors.gold};

            --gradient-start: {self.colors.gradient_start};
            --gradient-end: {self.colors.gradient_end};

            --font-family: {self.typography.font_family};
            --font-family-mono: {self.typography.font_family_mono};

            --spacing-xs: {self.spacing.xs};
            --spacing-sm: {self.spacing.sm};
            --spacing-md: {self.spacing.md};
            --spacing-lg: {self.spacing.lg};
            --spacing-xl: {self.spacing.xl};
            --spacing-xxl: {self.spacing.xxl};

            --shadow-sm: {self.shadows.sm};
            --shadow-md: {self.shadows.md};
            --shadow-lg: {self.shadows.lg};
            --shadow-xl: {self.shadows.xl};
            --shadow-glow: {self.shadows.glow};

            --radius-sm: {self.border_radius.sm};
            --radius-md: {self.border_radius.md};
            --radius-lg: {self.border_radius.lg};
            --radius-xl: {self.border_radius.xl};
            --radius-xxl: {self.border_radius.xxl};
            --radius-full: {self.border_radius.full};
        }}
        """


LIGHT_THEME = Theme(
    name="light",
    mode=ThemeMode.LIGHT,
    colors=ColorPalette(),
)

DARK_THEME = Theme(
    name="dark",
    mode=ThemeMode.DARK,
    colors=DarkColorPalette(),
)


def get_theme(mode: ThemeMode | str = ThemeMode.LIGHT) -> Theme:
    """Get theme by mode."""
    if isinstance(mode, str):
        mode = ThemeMode(mode)

    if mode == ThemeMode.DARK:
        return DARK_THEME
    return LIGHT_THEME


def get_streamlit_css(theme: Theme | None = None) -> str:
    """Generate complete Streamlit CSS with professional theme support."""
    theme = theme or LIGHT_THEME

    css_vars = theme.get_css_variables()

    return f"""
    <style>
    {css_vars}

    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    .stApp {{
        font-family: var(--font-family);
        background: var(--background);
    }}

    h1, h2, h3, h4, h5, h6 {{
        font-family: var(--font-family);
        color: var(--text-primary);
        font-weight: 600;
        letter-spacing: -0.02em;
    }}

    h1 {{
        font-size: {theme.typography.font_size_3xl};
        font-weight: 700;
    }}

    h2 {{
        font-size: {theme.typography.font_size_2xl};
    }}

    h3 {{
        font-size: {theme.typography.font_size_xl};
    }}

    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #0F172A 0%, #1E293B 100%);
        border-right: none;
        padding-top: 1rem;
    }}

    [data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
        padding: 1rem 0.75rem;
    }}

    /* All sidebar text should be bright and readable */
    [data-testid="stSidebar"] * {{
        color: #E2E8F0 !important;
    }}

    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {{
        color: #F8FAFC !important;
        font-weight: 500 !important;
    }}

    /* Sidebar navigation labels */
    [data-testid="stSidebar"] .stRadio > div {{
        gap: 0.375rem;
    }}

    [data-testid="stSidebar"] .stRadio > div > label {{
        padding: 0.875rem 1rem;
        border-radius: var(--radius-lg);
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        margin: 0;
        font-size: 0.9375rem !important;
        font-weight: 500 !important;
        color: #CBD5E1 !important;
        background: transparent;
        border: 1px solid transparent;
    }}

    [data-testid="stSidebar"] .stRadio > div > label:hover {{
        background: rgba(255, 255, 255, 0.08);
        color: #FFFFFF !important;
        border-color: rgba(255, 255, 255, 0.1);
    }}

    [data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] {{
        background: linear-gradient(135deg, var(--primary) 0%, #0052CC 100%);
        color: white !important;
        font-weight: 600 !important;
        box-shadow: 0 4px 12px rgba(0, 102, 255, 0.3);
        border-color: transparent;
    }}

    /* Sidebar section headers */
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {{
        color: #FFFFFF !important;
        font-weight: 700 !important;
        letter-spacing: -0.02em;
    }}

    /* Sidebar dividers */
    [data-testid="stSidebar"] hr {{
        border-color: rgba(255, 255, 255, 0.12);
        margin: 1.25rem 0;
    }}

    /* Sidebar caption/muted text */
    [data-testid="stSidebar"] .stCaption,
    [data-testid="stSidebar"] small {{
        color: #94A3B8 !important;
    }}

    /* Sidebar inputs */
    [data-testid="stSidebar"] .stTextInput input {{
        background: rgba(255, 255, 255, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.15);
        color: #FFFFFF !important;
        border-radius: var(--radius-lg);
        transition: all 0.2s ease;
    }}

    [data-testid="stSidebar"] .stTextInput input:focus {{
        background: rgba(255, 255, 255, 0.12);
        border-color: var(--primary);
        box-shadow: 0 0 0 3px rgba(0, 102, 255, 0.2);
    }}

    [data-testid="stSidebar"] .stTextInput input::placeholder {{
        color: rgba(255, 255, 255, 0.4);
    }}

    /* Sidebar buttons */
    [data-testid="stSidebar"] .stButton > button {{
        background: rgba(255, 255, 255, 0.08) !important;
        border: 1px solid rgba(255, 255, 255, 0.15) !important;
        color: #F8FAFC !important;
    }}

    [data-testid="stSidebar"] .stButton > button:hover {{
        background: rgba(255, 255, 255, 0.15) !important;
        border-color: rgba(255, 255, 255, 0.25) !important;
    }}

    [data-testid="stSidebar"] .stButton > button[kind="primary"] {{
        background: linear-gradient(135deg, var(--primary) 0%, #0052CC 100%) !important;
        border: none !important;
    }}

    .main .block-container {{
        padding: 2rem 3rem;
        max-width: 1400px;
    }}

    .stButton > button[kind="primary"],
    .stButton > button[data-testid="baseButton-primary"] {{
        background: linear-gradient(135deg, var(--primary) 0%, #0052CC 100%);
        color: var(--text-inverse);
        border: none;
        border-radius: var(--radius-lg);
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        font-size: 0.875rem;
        transition: all 0.2s ease;
        box-shadow: 0 2px 4px rgba(0, 102, 255, 0.2);
    }}

    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="baseButton-primary"]:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0, 102, 255, 0.3);
    }}

    .stButton > button {{
        background: var(--surface);
        color: var(--text-primary);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: 0.75rem 1.5rem;
        font-weight: 500;
        font-size: 0.875rem;
        transition: all 0.2s ease;
    }}

    .stButton > button:hover {{
        background: var(--surface-hover);
        border-color: var(--primary);
        color: var(--primary);
    }}

    .stTextInput > div > div > input,
    .stSelectbox > div > div > div,
    .stMultiSelect > div > div > div,
    .stTextArea textarea {{
        background: var(--background);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        color: var(--text-primary);
        padding: 0.75rem 1rem;
        font-size: 0.875rem;
        transition: all 0.2s ease;
    }}

    .stTextInput > div > div > input:focus,
    .stTextArea textarea:focus {{
        border-color: var(--primary);
        box-shadow: 0 0 0 3px var(--primary-light);
        outline: none;
    }}

    [data-testid="stMetricLabel"] {{
        color: var(--text-secondary);
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    [data-testid="stMetricValue"] {{
        color: var(--text-primary);
        font-size: 2rem;
        font-weight: 700;
        letter-spacing: -0.02em;
    }}

    [data-testid="stMetricDelta"] {{
        font-size: 0.875rem;
        font-weight: 600;
    }}

    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        background: var(--surface);
        padding: 0.25rem;
        border-radius: var(--radius-xl);
        border: 1px solid var(--border);
    }}

    .stTabs [data-baseweb="tab"] {{
        border-radius: var(--radius-lg);
        padding: 0.625rem 1.25rem;
        color: var(--text-secondary);
        font-weight: 500;
        font-size: 0.875rem;
        border: none;
        background: transparent;
    }}

    .stTabs [aria-selected="true"] {{
        background: var(--background);
        color: var(--primary);
        box-shadow: var(--shadow-sm);
    }}

    .stDataFrame {{
        border: 1px solid var(--border);
        border-radius: var(--radius-xl);
        overflow: hidden;
    }}

    .stDataFrame thead th {{
        background: var(--surface);
        color: var(--text-primary);
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        border-bottom: 2px solid var(--border);
        padding: 1rem;
    }}

    .stDataFrame tbody tr {{
        transition: background 0.15s ease;
    }}

    .stDataFrame tbody tr:hover {{
        background: var(--surface);
    }}

    .stDataFrame tbody td {{
        padding: 0.875rem 1rem;
        font-size: 0.875rem;
    }}

    .stAlert {{
        border-radius: var(--radius-lg);
        border: none;
        padding: 1rem 1.25rem;
    }}

    .streamlit-expanderHeader {{
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        color: var(--text-primary);
        font-weight: 500;
        padding: 1rem;
    }}

    .streamlit-expanderHeader:hover {{
        background: var(--surface-hover);
        border-color: var(--primary);
    }}

    .stProgress > div > div {{
        background: linear-gradient(90deg, var(--primary) 0%, var(--accent) 100%);
        border-radius: var(--radius-full);
    }}

    .stProgress > div {{
        background: var(--surface);
        border-radius: var(--radius-full);
    }}

    .status-badge {{
        display: inline-flex;
        align-items: center;
        padding: 0.375rem 0.75rem;
        border-radius: var(--radius-full);
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    .status-success {{
        background: var(--success-light);
        color: var(--success);
    }}

    .status-warning {{
        background: var(--warning-light);
        color: #92400E;
    }}

    .status-danger {{
        background: var(--danger-light);
        color: var(--danger);
    }}

    .status-info {{
        background: var(--info-light);
        color: var(--info);
    }}

    .tier-badge {{
        display: inline-flex;
        align-items: center;
        gap: 0.375rem;
        padding: 0.375rem 0.875rem;
        border-radius: var(--radius-full);
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    .tier-bronze {{
        background: linear-gradient(135deg, #B45309 0%, #92400E 100%);
        color: white;
    }}

    .tier-silver {{
        background: linear-gradient(135deg, #6B7280 0%, #4B5563 100%);
        color: white;
    }}

    .tier-gold {{
        background: linear-gradient(135deg, #D97706 0%, #B45309 100%);
        color: white;
    }}

    code {{
        font-family: var(--font-family-mono);
        background: var(--surface);
        padding: 0.2em 0.4em;
        border-radius: var(--radius-sm);
        font-size: 0.85em;
        color: var(--primary);
    }}

    pre {{
        font-family: var(--font-family-mono);
        background: #0F172A;
        color: #E2E8F0;
        padding: 1.25rem;
        border-radius: var(--radius-lg);
        border: none;
        overflow-x: auto;
    }}

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
        background: var(--secondary);
    }}

    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(-10px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}

    @keyframes slideIn {{
        from {{ opacity: 0; transform: translateX(-20px); }}
        to {{ opacity: 1; transform: translateX(0); }}
    }}

    @keyframes pulse {{
        0%, 100% {{ opacity: 1; }}
        50% {{ opacity: 0.6; }}
    }}

    @keyframes shimmer {{
        0% {{ background-position: -200% 0; }}
        100% {{ background-position: 200% 0; }}
    }}

    @keyframes gradientFlow {{
        0% {{ background-position: 0% 50%; }}
        50% {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}

    .animate-fade-in {{
        animation: fadeIn 0.3s ease-out;
    }}

    .animate-slide-in {{
        animation: slideIn 0.3s ease-out;
    }}

    .animate-pulse {{
        animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
    }}

    .gradient-text {{
        background: linear-gradient(135deg, var(--primary) 0%, var(--accent) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }}

    .gradient-border {{
        position: relative;
        background: var(--background);
        border-radius: var(--radius-xl);
    }}

    .gradient-border::before {{
        content: '';
        position: absolute;
        top: -2px;
        left: -2px;
        right: -2px;
        bottom: -2px;
        background: linear-gradient(135deg, var(--primary), var(--accent));
        border-radius: var(--radius-xl);
        z-index: -1;
    }}

    .card {{
        background: var(--background);
        border: 1px solid var(--border);
        border-radius: var(--radius-xl);
        padding: 1.5rem;
        box-shadow: var(--shadow-sm);
        transition: all 0.2s ease;
    }}

    .card:hover {{
        box-shadow: var(--shadow-md);
        border-color: var(--border);
    }}

    .glass-card {{
        background: rgba(255, 255, 255, 0.8);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.3);
        border-radius: var(--radius-xl);
    }}

    .metric-card {{
        background: linear-gradient(135deg, var(--surface) 0%, var(--background) 100%);
        border: 1px solid var(--border);
        border-radius: var(--radius-xl);
        padding: 1.25rem;
        transition: all 0.2s ease;
    }}

    .metric-card:hover {{
        transform: translateY(-2px);
        box-shadow: var(--shadow-md);
    }}

    .nav-item {{
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 0.75rem 1rem;
        border-radius: var(--radius-lg);
        color: rgba(255, 255, 255, 0.7);
        font-weight: 500;
        transition: all 0.2s ease;
        cursor: pointer;
    }}

    .nav-item:hover {{
        background: rgba(255, 255, 255, 0.1);
        color: white;
    }}

    .nav-item.active {{
        background: linear-gradient(135deg, var(--primary) 0%, var(--accent) 100%);
        color: white;
    }}

    .logo-text {{
        font-size: 1.5rem;
        font-weight: 700;
        letter-spacing: -0.03em;
    }}

    .logo-gradient {{
        background: linear-gradient(135deg, #0066FF 0%, #00D4AA 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }}

    /* Enhanced Button Styles */
    .btn {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 0.5rem;
        padding: 0.75rem 1.5rem;
        border-radius: var(--radius-lg);
        font-weight: 600;
        font-size: 0.875rem;
        cursor: pointer;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        border: none;
        text-decoration: none;
    }}

    .btn-primary {{
        background: linear-gradient(135deg, var(--primary) 0%, #0052CC 100%);
        color: white;
        box-shadow: 0 4px 14px 0 rgba(0, 102, 255, 0.25);
    }}

    .btn-primary:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 20px 0 rgba(0, 102, 255, 0.35);
    }}

    .btn-secondary {{
        background: var(--surface);
        color: var(--text-primary);
        border: 1px solid var(--border);
    }}

    .btn-secondary:hover {{
        background: var(--surface-hover);
        border-color: var(--primary);
        color: var(--primary);
    }}

    .btn-outline {{
        background: transparent;
        color: var(--primary);
        border: 2px solid var(--primary);
    }}

    .btn-outline:hover {{
        background: var(--primary);
        color: white;
    }}

    .btn-ghost {{
        background: transparent;
        color: var(--text-secondary);
    }}

    .btn-ghost:hover {{
        background: var(--surface);
        color: var(--text-primary);
    }}

    .btn-sm {{
        padding: 0.5rem 1rem;
        font-size: 0.8125rem;
    }}

    .btn-lg {{
        padding: 1rem 2rem;
        font-size: 1rem;
    }}

    .btn-icon {{
        padding: 0.625rem;
        border-radius: var(--radius-lg);
    }}

    /* Enhanced Card Styles */
    .card-elevated {{
        background: var(--background);
        border: 1px solid var(--border);
        border-radius: var(--radius-xl);
        padding: 1.5rem;
        box-shadow: var(--shadow-md);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }}

    .card-elevated:hover {{
        transform: translateY(-4px);
        box-shadow: var(--shadow-lg);
    }}

    .card-interactive {{
        cursor: pointer;
    }}

    .card-interactive:hover {{
        border-color: var(--primary);
    }}

    .card-gradient {{
        position: relative;
        overflow: hidden;
    }}

    .card-gradient::before {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, var(--primary), var(--accent));
    }}

    /* Data Table Enhancements */
    .table-modern {{
        width: 100%;
        border-collapse: collapse;
        font-size: 0.875rem;
    }}

    .table-modern th {{
        background: var(--surface);
        color: var(--text-secondary);
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 1rem;
        text-align: left;
        border-bottom: 2px solid var(--border);
    }}

    .table-modern td {{
        padding: 1rem;
        border-bottom: 1px solid var(--border-light);
        color: var(--text-primary);
    }}

    .table-modern tr:hover {{
        background: var(--surface);
    }}

    .table-modern tr:nth-child(even) {{
        background: rgba(248, 250, 252, 0.5);
    }}

    /* Form Input Enhancements */
    .input-group {{
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }}

    .input-label {{
        font-size: 0.875rem;
        font-weight: 500;
        color: var(--text-primary);
    }}

    .input-hint {{
        font-size: 0.75rem;
        color: var(--text-muted);
    }}

    .input-error {{
        font-size: 0.75rem;
        color: var(--danger);
        display: flex;
        align-items: center;
        gap: 0.25rem;
    }}

    .input-field {{
        padding: 0.75rem 1rem;
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        font-size: 0.9375rem;
        transition: all 0.2s ease;
        background: var(--background);
        color: var(--text-primary);
    }}

    .input-field:focus {{
        outline: none;
        border-color: var(--primary);
        box-shadow: 0 0 0 3px rgba(0, 102, 255, 0.1);
    }}

    .input-field.error {{
        border-color: var(--danger);
        box-shadow: 0 0 0 3px rgba(239, 68, 68, 0.1);
    }}

    .input-field.success {{
        border-color: var(--success);
    }}

    /* Alert/Notification Styles */
    .alert {{
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        padding: 1rem 1.25rem;
        border-radius: var(--radius-lg);
        font-size: 0.875rem;
    }}

    .alert-success {{
        background: var(--success-light);
        color: #065F46;
        border-left: 4px solid var(--success);
    }}

    .alert-warning {{
        background: var(--warning-light);
        color: #92400E;
        border-left: 4px solid var(--warning);
    }}

    .alert-error {{
        background: var(--danger-light);
        color: #991B1B;
        border-left: 4px solid var(--danger);
    }}

    .alert-info {{
        background: var(--info-light);
        color: #1E40AF;
        border-left: 4px solid var(--info);
    }}

    /* Toast Notifications */
    .toast {{
        position: fixed;
        bottom: 1.5rem;
        right: 1.5rem;
        padding: 1rem 1.5rem;
        border-radius: var(--radius-lg);
        background: var(--text-primary);
        color: white;
        box-shadow: var(--shadow-xl);
        display: flex;
        align-items: center;
        gap: 0.75rem;
        animation: slideInRight 0.3s ease-out;
        z-index: 9999;
    }}

    @keyframes slideInRight {{
        from {{ opacity: 0; transform: translateX(100%); }}
        to {{ opacity: 1; transform: translateX(0); }}
    }}

    /* Loading States */
    .skeleton {{
        background: linear-gradient(90deg, var(--surface) 0%, var(--surface-hover) 50%, var(--surface) 100%);
        background-size: 200% 100%;
        animation: shimmer 1.5s infinite;
        border-radius: var(--radius-md);
    }}

    .skeleton-text {{
        height: 1rem;
        margin-bottom: 0.5rem;
    }}

    .skeleton-title {{
        height: 1.5rem;
        width: 60%;
        margin-bottom: 1rem;
    }}

    .skeleton-card {{
        height: 120px;
        border-radius: var(--radius-xl);
    }}

    .spinner {{
        width: 24px;
        height: 24px;
        border: 3px solid var(--border);
        border-top-color: var(--primary);
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
    }}

    @keyframes spin {{
        to {{ transform: rotate(360deg); }}
    }}

    /* Progress Indicators */
    .progress-bar {{
        height: 8px;
        background: var(--surface);
        border-radius: var(--radius-full);
        overflow: hidden;
    }}

    .progress-fill {{
        height: 100%;
        background: linear-gradient(90deg, var(--primary), var(--accent));
        border-radius: var(--radius-full);
        transition: width 0.3s ease;
    }}

    /* Badge Styles */
    .badge {{
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.625rem;
        font-size: 0.75rem;
        font-weight: 600;
        border-radius: var(--radius-full);
        text-transform: uppercase;
        letter-spacing: 0.025em;
    }}

    .badge-primary {{
        background: var(--primary-light);
        color: var(--primary);
    }}

    .badge-success {{
        background: var(--success-light);
        color: var(--success);
    }}

    .badge-warning {{
        background: var(--warning-light);
        color: #B45309;
    }}

    .badge-danger {{
        background: var(--danger-light);
        color: var(--danger);
    }}

    .badge-neutral {{
        background: var(--surface);
        color: var(--text-secondary);
    }}

    /* Tooltip Styles */
    .tooltip {{
        position: relative;
    }}

    .tooltip::after {{
        content: attr(data-tooltip);
        position: absolute;
        bottom: 100%;
        left: 50%;
        transform: translateX(-50%) translateY(-8px);
        padding: 0.5rem 0.75rem;
        background: var(--text-primary);
        color: white;
        font-size: 0.75rem;
        border-radius: var(--radius-md);
        white-space: nowrap;
        opacity: 0;
        visibility: hidden;
        transition: all 0.2s ease;
        z-index: 100;
    }}

    .tooltip:hover::after {{
        opacity: 1;
        visibility: visible;
        transform: translateX(-50%) translateY(-4px);
    }}

    /* Divider Styles */
    .divider {{
        height: 1px;
        background: var(--border);
        margin: 1.5rem 0;
    }}

    .divider-vertical {{
        width: 1px;
        height: 100%;
        background: var(--border);
        margin: 0 1rem;
    }}

    /* Avatar Styles */
    .avatar {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: var(--radius-full);
        background: linear-gradient(135deg, var(--primary), var(--accent));
        color: white;
        font-weight: 600;
    }}

    .avatar-sm {{
        width: 32px;
        height: 32px;
        font-size: 0.75rem;
    }}

    .avatar-md {{
        width: 40px;
        height: 40px;
        font-size: 0.875rem;
    }}

    .avatar-lg {{
        width: 56px;
        height: 56px;
        font-size: 1.25rem;
    }}

    /* Stats Card */
    .stats-card {{
        background: var(--background);
        border: 1px solid var(--border);
        border-radius: var(--radius-xl);
        padding: 1.5rem;
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
        transition: all 0.2s ease;
    }}

    .stats-card:hover {{
        box-shadow: var(--shadow-md);
        transform: translateY(-2px);
    }}

    .stats-label {{
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    .stats-value {{
        font-size: 2rem;
        font-weight: 700;
        color: var(--text-primary);
        letter-spacing: -0.02em;
        line-height: 1;
    }}

    .stats-change {{
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
        font-size: 0.8125rem;
        font-weight: 600;
    }}

    .stats-change.positive {{
        color: var(--success);
    }}

    .stats-change.negative {{
        color: var(--danger);
    }}

    /* Icon Container */
    .icon-container {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: var(--radius-lg);
        flex-shrink: 0;
    }}

    .icon-sm {{
        width: 32px;
        height: 32px;
    }}

    .icon-md {{
        width: 40px;
        height: 40px;
    }}

    .icon-lg {{
        width: 48px;
        height: 48px;
    }}

    /* Quick Action Card */
    .quick-action {{
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 1rem 1.25rem;
        background: var(--background);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        cursor: pointer;
        transition: all 0.2s ease;
    }}

    .quick-action:hover {{
        background: var(--surface);
        border-color: var(--primary);
        transform: translateX(4px);
    }}

    .quick-action-icon {{
        width: 40px;
        height: 40px;
        border-radius: var(--radius-md);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.25rem;
    }}

    .quick-action-content {{
        flex: 1;
    }}

    .quick-action-title {{
        font-weight: 600;
        color: var(--text-primary);
        font-size: 0.9375rem;
    }}

    .quick-action-desc {{
        font-size: 0.8125rem;
        color: var(--text-muted);
    }}

    /* Empty State */
    .empty-state {{
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 3rem;
        text-align: center;
        color: var(--text-muted);
    }}

    .empty-state-icon {{
        font-size: 3rem;
        margin-bottom: 1rem;
        opacity: 0.5;
    }}

    .empty-state-title {{
        font-size: 1.125rem;
        font-weight: 600;
        color: var(--text-secondary);
        margin-bottom: 0.5rem;
    }}

    .empty-state-desc {{
        font-size: 0.875rem;
        max-width: 300px;
    }}

    /* Section Header */
    .section-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1.5rem;
    }}

    .section-title {{
        font-size: 1.25rem;
        font-weight: 600;
        color: var(--text-primary);
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }}

    .section-subtitle {{
        font-size: 0.875rem;
        color: var(--text-muted);
    }}

    /* Page Header */
    .page-header {{
        margin-bottom: 2rem;
    }}

    .page-title {{
        font-size: 1.75rem;
        font-weight: 700;
        color: var(--text-primary);
        letter-spacing: -0.02em;
        margin-bottom: 0.5rem;
    }}

    .page-description {{
        font-size: 1rem;
        color: var(--text-secondary);
        max-width: 600px;
    }}

    /* Breadcrumb */
    .breadcrumb {{
        display: flex;
        align-items: center;
        gap: 0.5rem;
        font-size: 0.875rem;
        color: var(--text-muted);
        margin-bottom: 1rem;
    }}

    .breadcrumb-item {{
        color: var(--text-muted);
        text-decoration: none;
    }}

    .breadcrumb-item:hover {{
        color: var(--primary);
    }}

    .breadcrumb-item.active {{
        color: var(--text-primary);
        font-weight: 500;
    }}

    .breadcrumb-separator {{
        color: var(--border);
    }}
    </style>
    """
