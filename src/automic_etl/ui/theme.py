"""Theme system for Automic ETL UI."""

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
    """Color palette for a theme."""
    # Primary colors
    primary: str = "#1f77b4"
    primary_hover: str = "#1a5f8f"
    primary_light: str = "#4fa3d6"

    # Secondary colors
    secondary: str = "#6c757d"
    secondary_hover: str = "#5a6268"

    # Accent colors
    accent: str = "#17a2b8"
    accent_hover: str = "#138496"

    # Status colors
    success: str = "#28a745"
    success_light: str = "#d4edda"
    warning: str = "#ffc107"
    warning_light: str = "#fff3cd"
    danger: str = "#dc3545"
    danger_light: str = "#f8d7da"
    info: str = "#17a2b8"
    info_light: str = "#d1ecf1"

    # Neutral colors
    background: str = "#ffffff"
    surface: str = "#f8f9fa"
    surface_hover: str = "#e9ecef"
    border: str = "#dee2e6"
    border_light: str = "#e9ecef"

    # Text colors
    text_primary: str = "#212529"
    text_secondary: str = "#6c757d"
    text_muted: str = "#adb5bd"
    text_inverse: str = "#ffffff"

    # Data tier colors (lakehouse specific)
    bronze: str = "#cd7f32"
    silver: str = "#c0c0c0"
    gold: str = "#ffd700"


@dataclass
class DarkColorPalette(ColorPalette):
    """Dark theme color palette."""
    # Override for dark mode
    background: str = "#1a1a2e"
    surface: str = "#16213e"
    surface_hover: str = "#0f3460"
    border: str = "#2d3748"
    border_light: str = "#4a5568"

    text_primary: str = "#f7fafc"
    text_secondary: str = "#a0aec0"
    text_muted: str = "#718096"

    # Adjusted status colors for dark mode
    success_light: str = "#1e4620"
    warning_light: str = "#4a3f00"
    danger_light: str = "#4a1a1a"
    info_light: str = "#1a3a4a"


@dataclass
class Typography:
    """Typography settings."""
    font_family: str = "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
    font_family_mono: str = "'JetBrains Mono', 'Fira Code', 'Consolas', monospace"

    # Font sizes
    font_size_xs: str = "0.75rem"
    font_size_sm: str = "0.875rem"
    font_size_base: str = "1rem"
    font_size_lg: str = "1.125rem"
    font_size_xl: str = "1.25rem"
    font_size_2xl: str = "1.5rem"
    font_size_3xl: str = "1.875rem"
    font_size_4xl: str = "2.25rem"

    # Font weights
    font_weight_light: int = 300
    font_weight_normal: int = 400
    font_weight_medium: int = 500
    font_weight_semibold: int = 600
    font_weight_bold: int = 700

    # Line heights
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
    """Shadow definitions."""
    sm: str = "0 1px 2px 0 rgba(0, 0, 0, 0.05)"
    md: str = "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)"
    lg: str = "0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)"
    xl: str = "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)"


@dataclass
class BorderRadius:
    """Border radius scale."""
    none: str = "0"
    sm: str = "0.25rem"
    md: str = "0.375rem"
    lg: str = "0.5rem"
    xl: str = "0.75rem"
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
            /* Primary colors */
            --primary: {self.colors.primary};
            --primary-hover: {self.colors.primary_hover};
            --primary-light: {self.colors.primary_light};

            /* Secondary colors */
            --secondary: {self.colors.secondary};
            --secondary-hover: {self.colors.secondary_hover};

            /* Accent colors */
            --accent: {self.colors.accent};
            --accent-hover: {self.colors.accent_hover};

            /* Status colors */
            --success: {self.colors.success};
            --success-light: {self.colors.success_light};
            --warning: {self.colors.warning};
            --warning-light: {self.colors.warning_light};
            --danger: {self.colors.danger};
            --danger-light: {self.colors.danger_light};
            --info: {self.colors.info};
            --info-light: {self.colors.info_light};

            /* Neutral colors */
            --background: {self.colors.background};
            --surface: {self.colors.surface};
            --surface-hover: {self.colors.surface_hover};
            --border: {self.colors.border};
            --border-light: {self.colors.border_light};

            /* Text colors */
            --text-primary: {self.colors.text_primary};
            --text-secondary: {self.colors.text_secondary};
            --text-muted: {self.colors.text_muted};
            --text-inverse: {self.colors.text_inverse};

            /* Data tier colors */
            --bronze: {self.colors.bronze};
            --silver: {self.colors.silver};
            --gold: {self.colors.gold};

            /* Typography */
            --font-family: {self.typography.font_family};
            --font-family-mono: {self.typography.font_family_mono};

            /* Spacing */
            --spacing-xs: {self.spacing.xs};
            --spacing-sm: {self.spacing.sm};
            --spacing-md: {self.spacing.md};
            --spacing-lg: {self.spacing.lg};
            --spacing-xl: {self.spacing.xl};
            --spacing-xxl: {self.spacing.xxl};

            /* Shadows */
            --shadow-sm: {self.shadows.sm};
            --shadow-md: {self.shadows.md};
            --shadow-lg: {self.shadows.lg};
            --shadow-xl: {self.shadows.xl};

            /* Border radius */
            --radius-sm: {self.border_radius.sm};
            --radius-md: {self.border_radius.md};
            --radius-lg: {self.border_radius.lg};
            --radius-xl: {self.border_radius.xl};
            --radius-full: {self.border_radius.full};
        }}
        """


# Predefined themes
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
    """Generate complete Streamlit CSS with theme support."""
    theme = theme or LIGHT_THEME

    css_vars = theme.get_css_variables()

    return f"""
    <style>
    {css_vars}

    /* Import fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* Global styles */
    .stApp {{
        font-family: var(--font-family);
        background-color: var(--background);
    }}

    /* Headers */
    h1, h2, h3, h4, h5, h6 {{
        font-family: var(--font-family);
        color: var(--text-primary);
        font-weight: 600;
    }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, var(--surface) 0%, var(--background) 100%);
        border-right: 1px solid var(--border-light);
    }}

    [data-testid="stSidebar"] .stMarkdown {{
        color: var(--text-primary);
    }}

    /* Main content area */
    .main .block-container {{
        padding: var(--spacing-lg) var(--spacing-xl);
        max-width: 1200px;
    }}

    /* Cards */
    .stCard, [data-testid="stMetricValue"] {{
        background: var(--surface);
        border: 1px solid var(--border-light);
        border-radius: var(--radius-lg);
        padding: var(--spacing-md);
        box-shadow: var(--shadow-sm);
    }}

    /* Buttons - Primary */
    .stButton > button[kind="primary"],
    .stButton > button[data-testid="baseButton-primary"] {{
        background: linear-gradient(135deg, var(--primary) 0%, var(--primary-hover) 100%);
        color: var(--text-inverse);
        border: none;
        border-radius: var(--radius-md);
        padding: var(--spacing-sm) var(--spacing-md);
        font-weight: 500;
        transition: all 0.2s ease;
        box-shadow: var(--shadow-sm);
    }}

    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="baseButton-primary"]:hover {{
        background: linear-gradient(135deg, var(--primary-hover) 0%, var(--primary) 100%);
        box-shadow: var(--shadow-md);
        transform: translateY(-1px);
    }}

    /* Buttons - Secondary */
    .stButton > button[kind="secondary"],
    .stButton > button {{
        background: var(--surface);
        color: var(--text-primary);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: var(--spacing-sm) var(--spacing-md);
        font-weight: 500;
        transition: all 0.2s ease;
    }}

    .stButton > button[kind="secondary"]:hover,
    .stButton > button:hover {{
        background: var(--surface-hover);
        border-color: var(--primary);
    }}

    /* Input fields */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div,
    .stMultiSelect > div > div > div,
    .stTextArea textarea {{
        background: var(--background);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        color: var(--text-primary);
        padding: var(--spacing-sm);
        transition: border-color 0.2s ease, box-shadow 0.2s ease;
    }}

    .stTextInput > div > div > input:focus,
    .stTextArea textarea:focus {{
        border-color: var(--primary);
        box-shadow: 0 0 0 3px var(--primary-light);
        outline: none;
    }}

    /* Metrics */
    [data-testid="stMetricLabel"] {{
        color: var(--text-secondary);
        font-size: {theme.typography.font_size_sm};
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    [data-testid="stMetricValue"] {{
        color: var(--text-primary);
        font-size: {theme.typography.font_size_2xl};
        font-weight: 700;
    }}

    [data-testid="stMetricDelta"] {{
        font-size: {theme.typography.font_size_sm};
        font-weight: 500;
    }}

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: var(--spacing-xs);
        background: var(--surface);
        padding: var(--spacing-xs);
        border-radius: var(--radius-lg);
    }}

    .stTabs [data-baseweb="tab"] {{
        border-radius: var(--radius-md);
        padding: var(--spacing-sm) var(--spacing-md);
        color: var(--text-secondary);
        font-weight: 500;
    }}

    .stTabs [aria-selected="true"] {{
        background: var(--primary);
        color: var(--text-inverse);
    }}

    /* Tables */
    .stDataFrame {{
        border: 1px solid var(--border-light);
        border-radius: var(--radius-lg);
        overflow: hidden;
    }}

    .stDataFrame thead th {{
        background: var(--surface);
        color: var(--text-primary);
        font-weight: 600;
        border-bottom: 2px solid var(--border);
    }}

    .stDataFrame tbody tr:hover {{
        background: var(--surface-hover);
    }}

    /* Alerts */
    .stAlert {{
        border-radius: var(--radius-md);
        border: none;
        padding: var(--spacing-md);
    }}

    .stAlert[data-baseweb="notification"] {{
        border-left: 4px solid var(--info);
        background: var(--info-light);
    }}

    /* Success alert */
    [data-testid="stAlert"]:has([data-testid="stMarkdownContainer"]:contains("success")) {{
        border-left: 4px solid var(--success);
        background: var(--success-light);
    }}

    /* Warning alert */
    [data-testid="stAlert"]:has([data-testid="stMarkdownContainer"]:contains("warning")) {{
        border-left: 4px solid var(--warning);
        background: var(--warning-light);
    }}

    /* Error alert */
    [data-testid="stAlert"]:has([data-testid="stMarkdownContainer"]:contains("error")) {{
        border-left: 4px solid var(--danger);
        background: var(--danger-light);
    }}

    /* Expanders */
    .streamlit-expanderHeader {{
        background: var(--surface);
        border: 1px solid var(--border-light);
        border-radius: var(--radius-md);
        color: var(--text-primary);
        font-weight: 500;
    }}

    .streamlit-expanderHeader:hover {{
        background: var(--surface-hover);
        border-color: var(--primary);
    }}

    /* Progress bars */
    .stProgress > div > div {{
        background: var(--primary);
        border-radius: var(--radius-full);
    }}

    /* Status badges */
    .status-badge {{
        display: inline-flex;
        align-items: center;
        padding: var(--spacing-xs) var(--spacing-sm);
        border-radius: var(--radius-full);
        font-size: {theme.typography.font_size_sm};
        font-weight: 500;
        text-transform: capitalize;
    }}

    .status-success {{
        background: var(--success-light);
        color: var(--success);
    }}

    .status-warning {{
        background: var(--warning-light);
        color: #856404;
    }}

    .status-danger {{
        background: var(--danger-light);
        color: var(--danger);
    }}

    .status-info {{
        background: var(--info-light);
        color: var(--info);
    }}

    /* Data tier badges */
    .tier-badge {{
        display: inline-flex;
        align-items: center;
        padding: var(--spacing-xs) var(--spacing-sm);
        border-radius: var(--radius-full);
        font-size: {theme.typography.font_size_sm};
        font-weight: 600;
    }}

    .tier-bronze {{
        background: linear-gradient(135deg, #cd7f32 0%, #8b5a2b 100%);
        color: white;
    }}

    .tier-silver {{
        background: linear-gradient(135deg, #c0c0c0 0%, #a9a9a9 100%);
        color: #333;
    }}

    .tier-gold {{
        background: linear-gradient(135deg, #ffd700 0%, #daa520 100%);
        color: #333;
    }}

    /* Code blocks */
    code {{
        font-family: var(--font-family-mono);
        background: var(--surface);
        padding: 0.2em 0.4em;
        border-radius: var(--radius-sm);
        font-size: {theme.typography.font_size_sm};
    }}

    pre {{
        font-family: var(--font-family-mono);
        background: var(--surface);
        padding: var(--spacing-md);
        border-radius: var(--radius-md);
        border: 1px solid var(--border-light);
        overflow-x: auto;
    }}

    /* Scrollbar */
    ::-webkit-scrollbar {{
        width: 8px;
        height: 8px;
    }}

    ::-webkit-scrollbar-track {{
        background: var(--surface);
        border-radius: var(--radius-full);
    }}

    ::-webkit-scrollbar-thumb {{
        background: var(--border);
        border-radius: var(--radius-full);
    }}

    ::-webkit-scrollbar-thumb:hover {{
        background: var(--secondary);
    }}

    /* Animations */
    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(-10px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}

    @keyframes pulse {{
        0%, 100% {{ opacity: 1; }}
        50% {{ opacity: 0.5; }}
    }}

    @keyframes spin {{
        from {{ transform: rotate(0deg); }}
        to {{ transform: rotate(360deg); }}
    }}

    .animate-fade-in {{
        animation: fadeIn 0.3s ease-out;
    }}

    .animate-pulse {{
        animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
    }}

    .animate-spin {{
        animation: spin 1s linear infinite;
    }}
    </style>
    """
