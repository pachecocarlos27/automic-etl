"""Data Processing Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
import polars as pl
from pathlib import Path
from typing import Any
import json


def show_data_processing_page():
    """Display the data processing page with Material Design."""
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <h1 style="font-size: 1.75rem; font-weight: 700; color: #212121; margin: 0 0 0.5rem; letter-spacing: -0.03em; font-family: 'Inter', sans-serif;">Data Processing</h1>
        <p style="font-size: 1rem; color: #757575; margin: 0; font-family: 'Inter', sans-serif;">Load datasets, redact sensitive information, and prepare data for distribution</p>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs([
        "Hugging Face Import",
        "Text Redaction",
        "Audio Processing",
        "Dataset Curation",
    ])

    with tab1:
        show_huggingface_import_section()

    with tab2:
        show_redaction_section()

    with tab3:
        show_audio_processing_section()

    with tab4:
        show_dataset_curation_section()


def show_huggingface_import_section():
    """Show Hugging Face dataset import section."""
    st.subheader("Import from Hugging Face")

    st.markdown("""
    Load datasets directly from [Hugging Face Hub](https://huggingface.co/datasets).
    Supports text, tabular, audio, and image datasets.
    """)

    col1, col2 = st.columns(2)

    with col1:
        dataset_name = st.text_input(
            "Dataset Name",
            placeholder="username/dataset-name",
            help="Full dataset path from Hugging Face Hub",
        )

        subset = st.text_input(
            "Subset (optional)",
            placeholder="e.g., default, en, train",
            help="Dataset configuration/subset name",
        )

        split = st.selectbox(
            "Split",
            ["train", "test", "validation", "all"],
            help="Which data split to load",
        )

    with col2:
        sample_size = st.number_input(
            "Sample Size",
            min_value=1,
            max_value=100000,
            value=50,
            help="Number of records to load (for testing)",
        )

        streaming = st.checkbox(
            "Enable Streaming",
            value=False,
            help="Stream data instead of downloading (for large datasets)",
        )

        trust_remote_code = st.checkbox(
            "Trust Remote Code",
            value=False,
            help="Required for some datasets with custom loading scripts",
        )

    st.markdown("---")

    col1, col2 = st.columns([1, 3])

    with col1:
        load_btn = st.button("Load Dataset", type="primary", disabled=not dataset_name)

    if load_btn and dataset_name:
        with st.spinner("Loading dataset from Hugging Face..."):
            try:
                from automic_etl.connectors.datasets.huggingface import (
                    HuggingFaceConfig,
                    HuggingFaceConnector,
                )

                from automic_etl.connectors.base import ConnectorType

                config = HuggingFaceConfig(
                    name=f"hf_{dataset_name.replace('/', '_')}",
                    connector_type=ConnectorType.API,
                    dataset_name=dataset_name,
                    subset=subset if subset else None,
                    split=split if split != "all" else "train",
                    streaming=streaming,
                    sample_size=sample_size,
                    trust_remote_code=trust_remote_code,
                )

                connector = HuggingFaceConnector(config)
                connector.connect()

                info = connector.get_info()
                result = connector.extract(limit=sample_size)

                st.session_state["hf_dataset"] = result.data
                st.session_state["hf_info"] = info

                st.success(f"Loaded {len(result.data):,} records from {dataset_name}")

                with st.expander("Dataset Info", expanded=True):
                    st.json(info)

                st.markdown("### Preview")
                st.dataframe(result.data.head(10), use_container_width=True)

                connector.disconnect()

            except Exception as e:
                st.error(f"Failed to load dataset: {str(e)}")

    if "hf_dataset" in st.session_state:
        st.markdown("---")
        st.markdown("### Loaded Dataset")
        df = st.session_state["hf_dataset"]
        st.info(f"**{len(df):,}** rows x **{len(df.columns)}** columns")

        if st.button("Use for Redaction"):
            st.session_state["redaction_source"] = df
            st.success("Dataset ready for redaction. Go to the Text Redaction tab.")


def show_redaction_section():
    """Show text redaction configuration section."""
    st.subheader("Text Redaction")

    st.markdown("""
    Configure patterns to detect and redact sensitive information from text data.
    Supports custom entity types, regex patterns, and multiple redaction strategies.
    """)

    with st.expander("Quick Start Patterns", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            use_common = st.checkbox("Common PII", value=True, help="Email, Phone, SSN, Credit Card")
        with col2:
            use_temporal = st.checkbox("Temporal", value=False, help="Days, Months, Dates")
        with col3:
            use_locations = st.checkbox("Locations", value=False, help="Cities, States")

    with st.expander("Custom Patterns"):
        st.markdown("Add custom terms to redact:")

        custom_entity = st.text_input("Entity Name", placeholder="e.g., PRODUCT_NAME")
        custom_terms = st.text_area(
            "Terms (one per line)",
            placeholder="Term1\nTerm2\nTerm3",
            height=100,
        )
        custom_tag = st.text_input("Replacement Tag", placeholder="[PRODUCT_NAME]")

        if "custom_patterns" not in st.session_state:
            st.session_state["custom_patterns"] = []

        if st.button("Add Pattern") and custom_entity and custom_terms:
            terms = [t.strip() for t in custom_terms.split("\n") if t.strip()]
            st.session_state["custom_patterns"].append({
                "name": custom_entity,
                "terms": terms,
                "tag": custom_tag or f"[{custom_entity.upper()}]",
            })
            st.success(f"Added pattern: {custom_entity} with {len(terms)} terms")

        if st.session_state.get("custom_patterns"):
            st.markdown("**Added Patterns:**")
            for i, pattern in enumerate(st.session_state["custom_patterns"]):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.text(f"{pattern['name']}: {len(pattern['terms'])} terms -> {pattern['tag']}")
                with col2:
                    if st.button("Remove", key=f"remove_pattern_{i}"):
                        st.session_state["custom_patterns"].pop(i)
                        st.rerun()

    st.markdown("---")

    strategy = st.selectbox(
        "Redaction Strategy",
        ["tag", "mask", "hash", "remove"],
        format_func=lambda x: {
            "tag": "Replace with Tag (e.g., [EMAIL])",
            "mask": "Mask with Characters (****)",
            "hash": "Replace with Hash",
            "remove": "Remove Completely",
        }.get(x, x),
    )

    text_col = None
    source_df = st.session_state.get("redaction_source") or st.session_state.get("hf_dataset")

    if source_df is not None:
        st.markdown("### Select Text Column")
        text_columns = [col for col in source_df.columns if source_df[col].dtype == pl.Utf8]

        if text_columns:
            text_col = st.selectbox("Text Column to Redact", text_columns)
        else:
            st.warning("No text columns found in the dataset.")
    else:
        st.info("Load a dataset from Hugging Face or upload a file to begin redaction.")

        uploaded = st.file_uploader("Or upload a file", type=["csv", "json", "parquet"])
        if uploaded:
            if uploaded.name.endswith(".csv"):
                source_df = pl.read_csv(uploaded)
            elif uploaded.name.endswith(".json"):
                source_df = pl.read_json(uploaded)
            else:
                source_df = pl.read_parquet(uploaded)
            st.session_state["redaction_source"] = source_df
            st.rerun()

    if source_df is not None and text_col:
        if st.button("Run Redaction", type="primary"):
            with st.spinner("Applying redaction..."):
                try:
                    from automic_etl.services.redaction import (
                        RedactionConfig,
                        RedactionService,
                        RedactionStrategy,
                        EntityPattern,
                    )

                    configs = []

                    if use_common:
                        configs.append(RedactionConfig.with_common_patterns())
                    if use_temporal:
                        configs.append(RedactionConfig.with_temporal_patterns())
                    if use_locations:
                        configs.append(RedactionConfig.with_location_patterns())

                    for custom in st.session_state.get("custom_patterns", []):
                        configs.append(RedactionConfig.with_custom_terms(
                            custom["name"],
                            custom["terms"],
                            custom["tag"],
                        ))

                    if not configs:
                        configs.append(RedactionConfig.with_common_patterns())

                    combined = configs[0]
                    for c in configs[1:]:
                        combined.merge(c)

                    combined.strategy = RedactionStrategy(strategy)

                    service = RedactionService(combined)

                    result_df, stats = service.redact_dataframe(
                        source_df,
                        columns=[text_col],
                    )

                    st.session_state["redacted_df"] = result_df
                    st.session_state["redaction_stats"] = stats

                    st.success(f"Redaction complete! {stats['total_redactions']:,} items redacted.")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Redactions", stats["total_redactions"])
                    with col2:
                        st.metric("Rows Processed", stats["rows_processed"])

                    if stats.get("entity_counts"):
                        st.markdown("**Entity Breakdown:**")
                        for entity, count in stats["entity_counts"].items():
                            st.text(f"  {entity}: {count}")

                    st.markdown("### Sample Comparison")
                    for i in range(min(3, len(result_df))):
                        with st.expander(f"Sample {i+1}"):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**Original:**")
                                st.text(str(result_df[text_col][i])[:500])
                            with col2:
                                st.markdown("**Redacted:**")
                                st.text(str(result_df[f"{text_col}_redacted"][i])[:500])

                except Exception as e:
                    st.error(f"Redaction failed: {str(e)}")


def show_audio_processing_section():
    """Show audio processing section."""
    st.subheader("Audio Processing")

    st.markdown("""
    Process audio files: extract metadata, mute or bleep segments, and prepare audio for distribution.
    """)

    col1, col2 = st.columns(2)

    with col1:
        audio_dir = st.text_input(
            "Audio Directory",
            placeholder="/path/to/audio/files",
            help="Directory containing audio files",
        )

        audio_format = st.multiselect(
            "Formats to Process",
            [".wav", ".mp3", ".flac", ".ogg", ".m4a"],
            default=[".wav", ".flac"],
        )

    with col2:
        operation = st.selectbox(
            "Operation",
            ["Extract Metadata", "Mute Segments", "Bleep Segments"],
        )

        output_dir = st.text_input(
            "Output Directory",
            placeholder="/path/to/output",
            help="Where to save processed files",
        )

    if operation in ["Mute Segments", "Bleep Segments"]:
        st.markdown("---")
        st.markdown("### Segment Timestamps")

        st.info("Enter segments to process (start and end times in milliseconds)")

        if "audio_segments" not in st.session_state:
            st.session_state["audio_segments"] = []

        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            start_ms = st.number_input("Start (ms)", min_value=0, value=0)
        with col2:
            end_ms = st.number_input("End (ms)", min_value=0, value=1000)
        with col3:
            if st.button("Add Segment"):
                st.session_state["audio_segments"].append({
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                })
                st.success("Segment added")

        if st.session_state["audio_segments"]:
            st.markdown("**Segments to process:**")
            for i, seg in enumerate(st.session_state["audio_segments"]):
                st.text(f"  {i+1}. {seg['start_ms']}ms - {seg['end_ms']}ms")

    st.markdown("---")

    if st.button("Process Audio", type="primary", disabled=not audio_dir):
        with st.spinner("Processing audio files..."):
            try:
                from automic_etl.connectors.media.audio import AudioConfig, AudioConnector
                from automic_etl.connectors.base import ConnectorType

                config = AudioConfig(
                    name="audio_processor",
                    connector_type=ConnectorType.FILE,
                    path=audio_dir,
                    supported_formats=audio_format,
                )

                connector = AudioConnector(config)
                connector.connect()

                if operation == "Extract Metadata":
                    result = connector.extract()
                    st.success(f"Processed {len(result.data)} audio files")
                    st.dataframe(result.data, use_container_width=True)

                    total_duration = connector.get_total_duration()
                    st.metric("Total Duration", f"{total_duration:.1f} seconds")

                else:
                    st.info("Segment-based audio processing requires segments with timestamps.")
                    if st.session_state.get("audio_segments"):
                        st.success(f"Ready to process {len(st.session_state['audio_segments'])} segments")

                connector.disconnect()

            except Exception as e:
                st.error(f"Audio processing failed: {str(e)}")


def show_dataset_curation_section():
    """Show dataset curation section."""
    st.subheader("Dataset Curation")

    st.markdown("""
    Organize and package your processed data for distribution.
    Creates a structured output with metadata, QA reports, and train/test/val splits.
    """)

    source_df = st.session_state.get("redacted_df") or st.session_state.get("hf_dataset")

    if source_df is None:
        st.info("Process data in the previous tabs to enable curation.")
        return

    st.success(f"Ready to curate: {len(source_df):,} rows")

    col1, col2 = st.columns(2)

    with col1:
        dataset_name = st.text_input(
            "Dataset Name",
            value="curated_dataset",
            help="Name for the output dataset",
        )

        description = st.text_area(
            "Description",
            placeholder="Describe your dataset...",
            height=100,
        )

        output_dir = st.text_input(
            "Output Directory",
            value="curated_output",
            help="Where to save the curated dataset",
        )

    with col2:
        st.markdown("**Split Ratios:**")
        train_ratio = st.slider("Train", 0.0, 1.0, 0.8)
        test_ratio = st.slider("Test", 0.0, 1.0, 0.1)
        val_ratio = st.slider("Validation", 0.0, 1.0, 0.1)

        if abs(train_ratio + test_ratio + val_ratio - 1.0) > 0.01:
            st.warning("Ratios should sum to 1.0")

        include_qa = st.checkbox("Include QA Report", value=True)
        metadata_format = st.selectbox("Metadata Format", ["parquet", "json", "csv"])

    st.markdown("---")

    if st.button("Curate Dataset", type="primary"):
        with st.spinner("Curating dataset..."):
            try:
                from automic_etl.services.dataset_curator import CurationConfig, DatasetCurator

                config = CurationConfig(
                    output_dir=output_dir,
                    split_ratios={"train": train_ratio, "test": test_ratio, "val": val_ratio},
                    metadata_format=metadata_format,
                    include_qa_report=include_qa,
                )

                curator = DatasetCurator(config)

                qa_report = None
                if include_qa and "redaction_stats" in st.session_state:
                    qa_report = st.session_state["redaction_stats"]

                metadata = curator.curate(
                    data=source_df,
                    name=dataset_name,
                    description=description,
                    qa_report=qa_report,
                )

                st.success("Dataset curated successfully!")

                with st.expander("Dataset Metadata", expanded=True):
                    st.json(metadata.to_dict())

                manifest = curator.generate_manifest()
                st.markdown("### Output Structure")

                for subdir, files in manifest.get("files", {}).items():
                    st.markdown(f"**{subdir}/** ({len(files)} files)")

                st.info(f"Dataset saved to: {output_dir}/")

            except Exception as e:
                st.error(f"Curation failed: {str(e)}")
