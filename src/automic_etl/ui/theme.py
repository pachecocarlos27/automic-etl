"""Material Design 3 theme system for Automic ETL UI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from enum import Enum


class ThemeMode(str, Enum):
    """Theme modes."""
    LIGHT = "light"
    DARK = "dark"


@dataclass
class MaterialColorPalette:
    """Material Design 3 color palette with semantic tokens."""
    
    # Primary - Deep Indigo (professional, trustworthy)
    primary: str = "#3F51B5"
    primary_light: str = "#757CE8"
    primary_dark: str = "#002884"
    on_primary: str = "#FFFFFF"
    primary_container: str = "#E8EAF6"
    on_primary_container: str = "#1A237E"
    
    # Secondary - Teal (complementary accent)
    secondary: str = "#009688"
    secondary_light: str = "#52C7B8"
    secondary_dark: str = "#00675B"
    on_secondary: str = "#FFFFFF"
    secondary_container: str = "#E0F2F1"
    on_secondary_container: str = "#004D40"
    
    # Tertiary - Amber (for highlights, warnings)
    tertiary: str = "#FF8F00"
    tertiary_light: str = "#FFC046"
    tertiary_dark: str = "#C56000"
    on_tertiary: str = "#FFFFFF"
    tertiary_container: str = "#FFF8E1"
    on_tertiary_container: str = "#E65100"
    
    # Status colors (Material guidelines)
    error: str = "#D32F2F"
    error_light: str = "#FFCDD2"
    on_error: str = "#FFFFFF"
    error_container: str = "#FFEBEE"
    on_error_container: str = "#B71C1C"
    
    warning: str = "#ED6C02"
    warning_light: str = "#FFE0B2"
    on_warning: str = "#FFFFFF"
    warning_container: str = "#FFF3E0"
    on_warning_container: str = "#E65100"
    
    success: str = "#2E7D32"
    success_light: str = "#C8E6C9"
    on_success: str = "#FFFFFF"
    success_container: str = "#E8F5E9"
    on_success_container: str = "#1B5E20"
    
    info: str = "#0288D1"
    info_light: str = "#B3E5FC"
    on_info: str = "#FFFFFF"
    info_container: str = "#E1F5FE"
    on_info_container: str = "#01579B"
    
    # Surface colors
    background: str = "#FAFAFA"
    on_background: str = "#212121"
    surface: str = "#FFFFFF"
    on_surface: str = "#212121"
    surface_variant: str = "#F5F5F5"
    on_surface_variant: str = "#616161"
    surface_container: str = "#EEEEEE"
    surface_container_high: str = "#E0E0E0"
    surface_container_low: str = "#F5F5F5"
    
    # Outline and borders
    outline: str = "#BDBDBD"
    outline_variant: str = "#E0E0E0"
    
    # Inverse colors
    inverse_surface: str = "#303030"
    inverse_on_surface: str = "#F5F5F5"
    inverse_primary: str = "#9FA8DA"
    
    # Scrim and shadow
    scrim: str = "#000000"
    shadow: str = "#000000"
    
    # Medallion architecture colors
    bronze: str = "#8D6E63"
    bronze_light: str = "#EFEBE9"
    silver: str = "#78909C"
    silver_light: str = "#ECEFF1"
    gold: str = "#FFA000"
    gold_light: str = "#FFF8E1"


@dataclass
class DarkMaterialColorPalette(MaterialColorPalette):
    """Material Design 3 dark theme palette."""
    
    # Primary - Indigo (lightened for dark mode)
    primary: str = "#9FA8DA"
    primary_light: str = "#C5CAE9"
    primary_dark: str = "#5C6BC0"
    on_primary: str = "#1A237E"
    primary_container: str = "#3949AB"
    on_primary_container: str = "#E8EAF6"
    
    # Secondary - Teal (lightened for dark mode)
    secondary: str = "#80CBC4"
    secondary_light: str = "#B2DFDB"
    secondary_dark: str = "#4DB6AC"
    on_secondary: str = "#004D40"
    secondary_container: str = "#00796B"
    on_secondary_container: str = "#E0F2F1"
    
    # Tertiary - Amber
    tertiary: str = "#FFB74D"
    tertiary_light: str = "#FFE0B2"
    tertiary_dark: str = "#FFA726"
    on_tertiary: str = "#E65100"
    tertiary_container: str = "#F57C00"
    on_tertiary_container: str = "#FFF8E1"
    
    # Status colors (lightened for dark mode)
    error: str = "#EF9A9A"
    error_light: str = "#FFCDD2"
    on_error: str = "#B71C1C"
    error_container: str = "#C62828"
    on_error_container: str = "#FFEBEE"
    
    warning: str = "#FFCC80"
    warning_light: str = "#FFE0B2"
    on_warning: str = "#E65100"
    warning_container: str = "#EF6C00"
    on_warning_container: str = "#FFF3E0"
    
    success: str = "#A5D6A7"
    success_light: str = "#C8E6C9"
    on_success: str = "#1B5E20"
    success_container: str = "#388E3C"
    on_success_container: str = "#E8F5E9"
    
    info: str = "#81D4FA"
    info_light: str = "#B3E5FC"
    on_info: str = "#01579B"
    info_container: str = "#0277BD"
    on_info_container: str = "#E1F5FE"
    
    # Surface colors (dark mode)
    background: str = "#121212"
    on_background: str = "#E0E0E0"
    surface: str = "#1E1E1E"
    on_surface: str = "#E0E0E0"
    surface_variant: str = "#2C2C2C"
    on_surface_variant: str = "#BDBDBD"
    surface_container: str = "#252525"
    surface_container_high: str = "#303030"
    surface_container_low: str = "#1A1A1A"
    
    # Outline and borders (dark mode)
    outline: str = "#616161"
    outline_variant: str = "#424242"
    
    # Inverse colors
    inverse_surface: str = "#E0E0E0"
    inverse_on_surface: str = "#303030"
    inverse_primary: str = "#3F51B5"
    
    # Medallion colors (adjusted for dark mode)
    bronze: str = "#BCAAA4"
    bronze_light: str = "#4E342E"
    silver: str = "#B0BEC5"
    silver_light: str = "#37474F"
    gold: str = "#FFCA28"
    gold_light: str = "#5D4037"


@dataclass
class Theme:
    """Theme configuration."""
    mode: ThemeMode = ThemeMode.LIGHT
    colors: MaterialColorPalette = field(default_factory=MaterialColorPalette)


LIGHT_THEME = Theme(mode=ThemeMode.LIGHT, colors=MaterialColorPalette())
DARK_THEME = Theme(mode=ThemeMode.DARK, colors=DarkMaterialColorPalette())


def get_theme(mode: ThemeMode | str = ThemeMode.LIGHT) -> Theme:
    """Get theme by mode."""
    if isinstance(mode, str):
        mode = ThemeMode(mode)
    return DARK_THEME if mode == ThemeMode.DARK else LIGHT_THEME


def get_streamlit_css(theme: Theme | None = None) -> str:
    """Generate Material Design 3 Streamlit CSS."""
    theme = theme or LIGHT_THEME
    c = theme.colors
    is_dark = theme.mode == ThemeMode.DARK
    
    return f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    :root {{
        /* Material 3 Semantic Tokens */
        --md-sys-color-primary: {c.primary};
        --md-sys-color-primary-light: {c.primary_light};
        --md-sys-color-primary-dark: {c.primary_dark};
        --md-sys-color-on-primary: {c.on_primary};
        --md-sys-color-primary-container: {c.primary_container};
        --md-sys-color-on-primary-container: {c.on_primary_container};
        
        --md-sys-color-secondary: {c.secondary};
        --md-sys-color-secondary-light: {c.secondary_light};
        --md-sys-color-secondary-dark: {c.secondary_dark};
        --md-sys-color-on-secondary: {c.on_secondary};
        --md-sys-color-secondary-container: {c.secondary_container};
        --md-sys-color-on-secondary-container: {c.on_secondary_container};
        
        --md-sys-color-tertiary: {c.tertiary};
        --md-sys-color-on-tertiary: {c.on_tertiary};
        --md-sys-color-tertiary-container: {c.tertiary_container};
        
        --md-sys-color-error: {c.error};
        --md-sys-color-error-light: {c.error_light};
        --md-sys-color-on-error: {c.on_error};
        --md-sys-color-error-container: {c.error_container};
        --md-sys-color-on-error-container: {c.on_error_container};
        
        --md-sys-color-warning: {c.warning};
        --md-sys-color-warning-container: {c.warning_container};
        --md-sys-color-on-warning-container: {c.on_warning_container};
        
        --md-sys-color-success: {c.success};
        --md-sys-color-success-container: {c.success_container};
        --md-sys-color-on-success-container: {c.on_success_container};
        
        --md-sys-color-info: {c.info};
        --md-sys-color-info-container: {c.info_container};
        --md-sys-color-on-info-container: {c.on_info_container};
        
        --md-sys-color-background: {c.background};
        --md-sys-color-on-background: {c.on_background};
        --md-sys-color-surface: {c.surface};
        --md-sys-color-on-surface: {c.on_surface};
        --md-sys-color-surface-variant: {c.surface_variant};
        --md-sys-color-on-surface-variant: {c.on_surface_variant};
        --md-sys-color-surface-container: {c.surface_container};
        --md-sys-color-surface-container-high: {c.surface_container_high};
        --md-sys-color-surface-container-low: {c.surface_container_low};
        
        --md-sys-color-outline: {c.outline};
        --md-sys-color-outline-variant: {c.outline_variant};
        
        --md-sys-color-inverse-surface: {c.inverse_surface};
        --md-sys-color-inverse-on-surface: {c.inverse_on_surface};
        --md-sys-color-inverse-primary: {c.inverse_primary};
        
        /* Medallion colors */
        --md-custom-bronze: {c.bronze};
        --md-custom-bronze-container: {c.bronze_light};
        --md-custom-silver: {c.silver};
        --md-custom-silver-container: {c.silver_light};
        --md-custom-gold: {c.gold};
        --md-custom-gold-container: {c.gold_light};
        
        /* Material elevation shadows */
        --md-sys-elevation-1: 0 1px 2px rgba(0,0,0,0.05);
        --md-sys-elevation-2: 0 1px 3px rgba(0,0,0,0.1), 0 1px 2px rgba(0,0,0,0.06);
        --md-sys-elevation-3: 0 4px 6px rgba(0,0,0,0.1), 0 2px 4px rgba(0,0,0,0.06);
        --md-sys-elevation-4: 0 10px 15px rgba(0,0,0,0.1), 0 4px 6px rgba(0,0,0,0.05);
        --md-sys-elevation-5: 0 20px 25px rgba(0,0,0,0.1), 0 10px 10px rgba(0,0,0,0.04);
        
        /* Material shape tokens */
        --md-sys-shape-corner-none: 0px;
        --md-sys-shape-corner-xs: 4px;
        --md-sys-shape-corner-sm: 8px;
        --md-sys-shape-corner-md: 12px;
        --md-sys-shape-corner-lg: 16px;
        --md-sys-shape-corner-xl: 28px;
        --md-sys-shape-corner-full: 9999px;
        
        /* Legacy aliases for compatibility */
        --primary: var(--md-sys-color-primary);
        --primary-hover: var(--md-sys-color-primary-dark);
        --text-primary: var(--md-sys-color-on-surface);
        --text-secondary: var(--md-sys-color-on-surface-variant);
        --text-muted: var(--md-sys-color-outline);
        --bg: var(--md-sys-color-background);
        --surface: var(--md-sys-color-surface);
        --border: var(--md-sys-color-outline-variant);
        --success: var(--md-sys-color-success);
        --warning: var(--md-sys-color-warning);
        --danger: var(--md-sys-color-error);
    }}
    
    .stApp {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        background: var(--md-sys-color-background);
    }}
    
    /* Typography - Material type scale */
    h1, h2, h3, h4, h5, h6 {{
        font-family: 'Inter', sans-serif;
        color: var(--md-sys-color-on-surface);
        font-weight: 600;
        letter-spacing: -0.025em;
    }}
    
    h1 {{ font-size: 2rem; font-weight: 700; letter-spacing: -0.03em; }}
    h2 {{ font-size: 1.5rem; font-weight: 600; }}
    h3 {{ font-size: 1.25rem; font-weight: 600; }}
    h4 {{ font-size: 1.125rem; font-weight: 500; }}
    
    p, span, div {{ color: var(--md-sys-color-on-surface-variant); }}
    
    /* Sidebar - Material surface with elevation */
    [data-testid="stSidebar"] {{
        background: var(--md-sys-color-inverse-surface);
        border-right: none;
        box-shadow: var(--md-sys-elevation-2);
    }}
    
    [data-testid="stSidebar"] * {{
        color: var(--md-sys-color-inverse-on-surface) !important;
    }}
    
    [data-testid="stSidebar"] .stRadio > div {{
        gap: 4px;
    }}
    
    [data-testid="stSidebar"] .stRadio > div > label {{
        padding: 0.875rem 1rem;
        border-radius: var(--md-sys-shape-corner-md);
        font-size: 0.875rem;
        font-weight: 500;
        color: rgba(255, 255, 255, 0.7) !important;
        background: transparent;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        margin: 0;
    }}
    
    [data-testid="stSidebar"] .stRadio > div > label:hover {{
        background: rgba(255, 255, 255, 0.08);
        color: rgba(255, 255, 255, 0.9) !important;
    }}
    
    [data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] {{
        background: var(--md-sys-color-primary);
        color: var(--md-sys-color-on-primary) !important;
        font-weight: 600;
        box-shadow: var(--md-sys-elevation-1);
    }}
    
    [data-testid="stSidebar"] hr {{
        border-color: rgba(255, 255, 255, 0.12);
        margin: 1rem 0;
    }}
    
    [data-testid="stSidebar"] .stTextInput input {{
        background: rgba(255, 255, 255, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.12);
        color: var(--md-sys-color-inverse-on-surface) !important;
        border-radius: var(--md-sys-shape-corner-md);
        transition: all 0.2s ease;
    }}
    
    [data-testid="stSidebar"] .stTextInput input:focus {{
        border-color: var(--md-sys-color-primary);
        background: rgba(255, 255, 255, 0.12);
    }}
    
    [data-testid="stSidebar"] .stTextInput input::placeholder {{
        color: rgba(255, 255, 255, 0.5);
    }}
    
    /* Main content area */
    .main .block-container {{
        padding: 2rem 3rem;
        max-width: 1280px;
    }}
    
    /* Material Buttons */
    .stButton > button {{
        background: var(--md-sys-color-surface);
        color: var(--md-sys-color-primary);
        border: 1px solid var(--md-sys-color-outline-variant);
        border-radius: var(--md-sys-shape-corner-full);
        padding: 0.625rem 1.5rem;
        font-weight: 500;
        font-size: 0.875rem;
        letter-spacing: 0.02em;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: none;
    }}
    
    .stButton > button:hover {{
        background: var(--md-sys-color-primary-container);
        border-color: var(--md-sys-color-primary);
        box-shadow: var(--md-sys-elevation-1);
    }}
    
    .stButton > button[kind="primary"],
    .stButton > button[data-testid="baseButton-primary"] {{
        background: var(--md-sys-color-primary);
        color: var(--md-sys-color-on-primary);
        border: none;
        box-shadow: var(--md-sys-elevation-1);
    }}
    
    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="baseButton-primary"]:hover {{
        background: var(--md-sys-color-primary-dark);
        box-shadow: var(--md-sys-elevation-2);
    }}
    
    /* Secondary button style */
    .stButton > button[kind="secondary"],
    .stButton > button[data-testid="baseButton-secondary"] {{
        background: var(--md-sys-color-secondary-container);
        color: var(--md-sys-color-on-secondary-container);
        border: none;
    }}
    
    .stButton > button[kind="secondary"]:hover,
    .stButton > button[data-testid="baseButton-secondary"]:hover {{
        background: var(--md-sys-color-secondary);
        color: var(--md-sys-color-on-secondary);
    }}
    
    /* Material Inputs */
    .stTextInput > div > div > input,
    .stSelectbox > div > div,
    .stTextArea textarea {{
        background: var(--md-sys-color-surface);
        border: 1px solid var(--md-sys-color-outline);
        border-radius: var(--md-sys-shape-corner-sm);
        color: var(--md-sys-color-on-surface);
        font-size: 0.875rem;
        transition: all 0.2s ease;
    }}
    
    .stTextInput > div > div > input:focus,
    .stTextArea textarea:focus {{
        border-color: var(--md-sys-color-primary);
        border-width: 2px;
        box-shadow: none;
        outline: none;
    }}
    
    .stTextInput > label,
    .stTextArea > label,
    .stSelectbox > label {{
        color: var(--md-sys-color-on-surface-variant);
        font-size: 0.875rem;
        font-weight: 500;
    }}
    
    /* Material Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        background: var(--md-sys-color-surface-container-low);
        padding: 4px;
        border-radius: var(--md-sys-shape-corner-lg);
        border: 1px solid var(--md-sys-color-outline-variant);
    }}
    
    .stTabs [data-baseweb="tab"] {{
        border-radius: var(--md-sys-shape-corner-md);
        padding: 0.625rem 1.25rem;
        color: var(--md-sys-color-on-surface-variant);
        font-weight: 500;
        font-size: 0.875rem;
        background: transparent;
        border: none;
        transition: all 0.2s ease;
    }}
    
    .stTabs [data-baseweb="tab"]:hover {{
        background: var(--md-sys-color-surface-container);
    }}
    
    .stTabs [aria-selected="true"] {{
        background: var(--md-sys-color-surface);
        color: var(--md-sys-color-primary);
        box-shadow: var(--md-sys-elevation-1);
    }}
    
    /* Material Metrics */
    [data-testid="stMetricLabel"] {{
        color: var(--md-sys-color-on-surface-variant);
        font-size: 0.75rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }}
    
    [data-testid="stMetricValue"] {{
        color: var(--md-sys-color-on-surface);
        font-size: 1.875rem;
        font-weight: 700;
        letter-spacing: -0.02em;
    }}
    
    [data-testid="stMetricDelta"] {{
        font-size: 0.75rem;
        font-weight: 500;
    }}
    
    /* Material Data tables */
    .stDataFrame {{
        border: 1px solid var(--md-sys-color-outline-variant);
        border-radius: var(--md-sys-shape-corner-md);
        overflow: hidden;
        box-shadow: var(--md-sys-elevation-1);
    }}
    
    .stDataFrame thead th {{
        background: var(--md-sys-color-surface-container);
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 0.875rem 1rem;
        color: var(--md-sys-color-on-surface);
    }}
    
    .stDataFrame tbody tr {{
        transition: background 0.15s ease;
    }}
    
    .stDataFrame tbody tr:hover {{
        background: var(--md-sys-color-surface-container-low);
    }}
    
    /* Material Expanders */
    .streamlit-expanderHeader {{
        background: var(--md-sys-color-surface);
        border: 1px solid var(--md-sys-color-outline-variant);
        border-radius: var(--md-sys-shape-corner-md);
        font-weight: 500;
        padding: 0.875rem 1rem;
        transition: all 0.2s ease;
    }}
    
    .streamlit-expanderHeader:hover {{
        background: var(--md-sys-color-surface-container-low);
        border-color: var(--md-sys-color-outline);
    }}
    
    /* Material Alerts */
    .stAlert {{
        border-radius: var(--md-sys-shape-corner-md);
        border: none;
        box-shadow: var(--md-sys-elevation-1);
    }}
    
    /* Material Progress bar */
    .stProgress > div > div {{
        background: linear-gradient(90deg, var(--md-sys-color-primary), var(--md-sys-color-primary-light));
        border-radius: var(--md-sys-shape-corner-full);
    }}
    
    .stProgress > div {{
        background: var(--md-sys-color-surface-container);
        border-radius: var(--md-sys-shape-corner-full);
    }}
    
    /* Material File uploader */
    [data-testid="stFileUploader"] {{
        border: 2px dashed var(--md-sys-color-outline);
        border-radius: var(--md-sys-shape-corner-lg);
        padding: 2rem;
        background: var(--md-sys-color-surface-container-low);
        transition: all 0.2s ease;
    }}
    
    [data-testid="stFileUploader"]:hover {{
        border-color: var(--md-sys-color-primary);
        background: var(--md-sys-color-primary-container);
    }}
    
    /* Code blocks */
    code {{
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        background: var(--md-sys-color-surface-container);
        padding: 0.125rem 0.5rem;
        border-radius: var(--md-sys-shape-corner-xs);
        font-size: 0.85em;
        color: var(--md-sys-color-secondary-dark);
    }}
    
    pre {{
        background: var(--md-sys-color-inverse-surface);
        color: var(--md-sys-color-inverse-on-surface);
        padding: 1rem;
        border-radius: var(--md-sys-shape-corner-md);
        border: none;
        box-shadow: var(--md-sys-elevation-2);
    }}
    
    /* Scrollbar */
    ::-webkit-scrollbar {{
        width: 8px;
        height: 8px;
    }}
    
    ::-webkit-scrollbar-track {{
        background: transparent;
    }}
    
    ::-webkit-scrollbar-thumb {{
        background: var(--md-sys-color-outline);
        border-radius: var(--md-sys-shape-corner-full);
    }}
    
    ::-webkit-scrollbar-thumb:hover {{
        background: var(--md-sys-color-on-surface-variant);
    }}
    
    /* Material Cards */
    .card {{
        background: var(--md-sys-color-surface);
        border: 1px solid var(--md-sys-color-outline-variant);
        border-radius: var(--md-sys-shape-corner-lg);
        padding: 1.5rem;
        box-shadow: var(--md-sys-elevation-1);
    }}
    
    .card-elevated {{
        background: var(--md-sys-color-surface);
        border: none;
        border-radius: var(--md-sys-shape-corner-lg);
        padding: 1.5rem;
        box-shadow: var(--md-sys-elevation-2);
    }}
    
    .card-filled {{
        background: var(--md-sys-color-surface-container);
        border: none;
        border-radius: var(--md-sys-shape-corner-lg);
        padding: 1.5rem;
    }}
    
    .metric-card {{
        background: var(--md-sys-color-surface);
        border: 1px solid var(--md-sys-color-outline-variant);
        border-radius: var(--md-sys-shape-corner-lg);
        padding: 1.25rem;
        transition: all 0.2s ease;
    }}
    
    .metric-card:hover {{
        box-shadow: var(--md-sys-elevation-2);
        border-color: var(--md-sys-color-outline);
    }}
    
    /* Material Status Chips */
    .status-chip {{
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.75rem;
        border-radius: var(--md-sys-shape-corner-sm);
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.02em;
    }}
    
    .status-success {{
        color: var(--md-sys-color-on-success-container);
        background: var(--md-sys-color-success-container);
    }}
    
    .status-warning {{
        color: var(--md-sys-color-on-warning-container);
        background: var(--md-sys-color-warning-container);
    }}
    
    .status-error, .status-danger {{
        color: var(--md-sys-color-on-error-container);
        background: var(--md-sys-color-error-container);
    }}
    
    .status-info {{
        color: var(--md-sys-color-on-info-container);
        background: var(--md-sys-color-info-container);
    }}
    
    .status-primary {{
        color: var(--md-sys-color-on-primary-container);
        background: var(--md-sys-color-primary-container);
    }}
    
    .status-secondary {{
        color: var(--md-sys-color-on-secondary-container);
        background: var(--md-sys-color-secondary-container);
    }}
    
    /* Medallion layer cards */
    .medallion-bronze {{
        border-left: 4px solid var(--md-custom-bronze);
        background: var(--md-custom-bronze-container);
    }}
    
    .medallion-silver {{
        border-left: 4px solid var(--md-custom-silver);
        background: var(--md-custom-silver-container);
    }}
    
    .medallion-gold {{
        border-left: 4px solid var(--md-custom-gold);
        background: var(--md-custom-gold-container);
    }}
    
    /* Material Dividers */
    hr {{
        border: none;
        border-top: 1px solid var(--md-sys-color-outline-variant);
        margin: 1.5rem 0;
    }}
    
    /* Focus states for accessibility */
    *:focus-visible {{
        outline: 2px solid var(--md-sys-color-primary);
        outline-offset: 2px;
    }}
    
    /* Material transitions */
    * {{
        transition-timing-function: cubic-bezier(0.4, 0, 0.2, 1);
    }}
    </style>
    """
