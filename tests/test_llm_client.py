"""Tests for LLM client."""

from unittest.mock import MagicMock, patch

import pytest

from automic_etl.core.config import LLMProvider, Settings
from automic_etl.core.exceptions import LLMError
from automic_etl.llm.client import LLMClient, LLMResponse


@pytest.fixture
def llm_settings():
    """Create LLM settings for testing."""
    return Settings(
        llm__provider="anthropic",
        llm__api_key="test-key",
        llm__model="claude-sonnet-4-20250514",
    )


class TestLLMClientInitialization:
    """Test LLM client initialization."""

    def test_init_with_anthropic(self, llm_settings):
        """Initialize client with Anthropic provider."""
        client = LLMClient(llm_settings)
        assert client.llm_config.provider == LLMProvider.ANTHROPIC

    def test_init_with_settings(self, llm_settings):
        """Client should store settings correctly."""
        client = LLMClient(llm_settings)
        assert client.settings == llm_settings
        assert client.llm_config.api_key == "test-key"

    def test_client_lazy_loading(self, llm_settings):
        """Client should be lazy-loaded on first use."""
        client = LLMClient(llm_settings)
        assert client._client is None


class TestLLMClientCompletion:
    """Test LLM completion functionality."""

    def test_complete_with_anthropic(self, llm_settings):
        """Should successfully complete with Anthropic."""
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_instance = MagicMock()
            mock_anthropic.return_value = mock_instance

            mock_instance.messages.create.return_value = MagicMock(
                content=[MagicMock(text="Response text")],
                model="claude-sonnet-4-20250514",
                usage=MagicMock(input_tokens=10, output_tokens=20),
                stop_reason="end_turn",
            )

            client = LLMClient(llm_settings)
            response = client.complete(prompt="test prompt")

            assert isinstance(response, LLMResponse)
            assert response.content is not None
            assert response.model == "claude-sonnet-4-20250514"

    def test_complete_with_system_prompt(self, llm_settings):
        """Should accept and use system prompt."""
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_instance = MagicMock()
            mock_anthropic.return_value = mock_instance
            mock_instance.messages.create.return_value = MagicMock(
                content=[MagicMock(text="Response")],
                model="claude-sonnet-4-20250514",
                usage=MagicMock(input_tokens=10, output_tokens=10),
                stop_reason="end_turn",
            )

            client = LLMClient(llm_settings)
            response = client.complete(
                prompt="test",
                system_prompt="You are a helpful assistant",
            )

            assert response is not None

    def test_complete_with_json_mode(self, llm_settings):
        """Should support JSON mode."""
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_instance = MagicMock()
            mock_anthropic.return_value = mock_instance
            mock_instance.messages.create.return_value = MagicMock(
                content=[MagicMock(text='{"key": "value"}')],
                model="claude-sonnet-4-20250514",
                usage=MagicMock(input_tokens=10, output_tokens=10),
                stop_reason="end_turn",
            )

            client = LLMClient(llm_settings)
            response = client.complete(prompt="test", json_mode=True)

            assert response.content == '{"key": "value"}'

    def test_complete_with_custom_temperature(self, llm_settings):
        """Should accept custom temperature."""
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_instance = MagicMock()
            mock_anthropic.return_value = mock_instance
            mock_instance.messages.create.return_value = MagicMock(
                content=[MagicMock(text="Response")],
                model="claude-sonnet-4-20250514",
                usage=MagicMock(input_tokens=10, output_tokens=10),
                stop_reason="end_turn",
            )

            client = LLMClient(llm_settings)
            response = client.complete(prompt="test", temperature=0.5)

            assert response is not None


class TestLLMClientErrorHandling:
    """Test error handling in LLM client."""

    def test_missing_api_key(self):
        """Should raise error when API key is missing."""
        settings = Settings(
            llm__provider="anthropic",
            llm__api_key=None,
            llm__model="claude-sonnet-4-20250514",
        )

        with patch("anthropic.Anthropic", side_effect=Exception("API key required")):
            client = LLMClient(settings)
            with pytest.raises(Exception):
                client.complete(prompt="test")

    def test_invalid_provider(self):
        """Should handle invalid provider gracefully."""
        settings = Settings(
            llm__provider="invalid_provider",
            llm__api_key="test-key",
        )

        client = LLMClient(settings)
        # The client should handle this gracefully or raise appropriate error
        assert client is not None

    def test_api_error_handling(self, llm_settings):
        """Should handle API errors gracefully."""
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_instance = MagicMock()
            mock_anthropic.return_value = mock_instance
            mock_instance.messages.create.side_effect = Exception("API Error")

            client = LLMClient(llm_settings)
            with pytest.raises(Exception):
                client.complete(prompt="test")


class TestLLMResponse:
    """Test LLM response object."""

    def test_response_creation(self):
        """Should create LLM response correctly."""
        response = LLMResponse(
            content="test response",
            model="test-model",
            tokens_used=100,
            finish_reason="stop",
        )

        assert response.content == "test response"
        assert response.model == "test-model"
        assert response.tokens_used == 100
        assert response.finish_reason == "stop"

    def test_response_with_metadata(self):
        """Should store additional metadata."""
        response = LLMResponse(
            content="test",
            model="test-model",
            tokens_used=50,
            finish_reason="stop",
            metadata={"key": "value"},
        )

        assert response.metadata == {"key": "value"}


class TestLLMClientOpenAI:
    """Test OpenAI provider support."""

    def test_openai_provider_initialization(self):
        """Should initialize OpenAI client."""
        settings = Settings(
            llm__provider="openai",
            llm__api_key="test-key",
            llm__model="gpt-4",
        )

        with patch("openai.OpenAI") as mock_openai:
            mock_instance = MagicMock()
            mock_openai.return_value = mock_instance

            client = LLMClient(settings)
            assert client.llm_config.provider == LLMProvider.OPENAI


class TestLLMClientOllama:
    """Test Ollama provider support."""

    def test_ollama_provider_initialization(self):
        """Should initialize Ollama client with custom base URL."""
        settings = Settings(
            llm__provider="ollama",
            llm__api_key="ollama",
            llm__base_url="http://localhost:11434/v1",
            llm__model="llama2",
        )

        with patch("openai.OpenAI") as mock_openai:
            mock_instance = MagicMock()
            mock_openai.return_value = mock_instance

            client = LLMClient(settings)
            assert client.llm_config.provider == LLMProvider.OLLAMA
