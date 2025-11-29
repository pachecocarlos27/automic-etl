"""LLM client abstraction for multiple providers."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import structlog

from automic_etl.core.config import LLMProvider, Settings
from automic_etl.core.exceptions import LLMError

logger = structlog.get_logger()


@dataclass
class LLMResponse:
    """Response from LLM."""

    content: str
    model: str
    tokens_used: int
    finish_reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


class LLMClient:
    """
    Unified LLM client supporting multiple providers.

    Uses LiteLLM for provider abstraction with native fallbacks.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.llm_config = settings.llm
        self.logger = logger.bind(
            component="llm_client",
            provider=self.llm_config.provider.value,
            model=self.llm_config.model,
        )
        self._client: Any = None

    def _get_client(self) -> Any:
        """Get or create the LLM client."""
        if self._client is not None:
            return self._client

        provider = self.llm_config.provider

        try:
            if provider == LLMProvider.ANTHROPIC:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.llm_config.api_key)
            elif provider == LLMProvider.OPENAI:
                import openai
                self._client = openai.OpenAI(api_key=self.llm_config.api_key)
            elif provider == LLMProvider.OLLAMA:
                # Ollama uses OpenAI-compatible API
                import openai
                self._client = openai.OpenAI(
                    api_key="ollama",
                    base_url=self.llm_config.base_url or "http://localhost:11434/v1",
                )
            else:
                # Use LiteLLM for other providers
                import litellm
                self._client = litellm
        except ImportError as e:
            raise LLMError(
                f"Missing dependency for {provider}: {str(e)}",
                provider=provider.value,
            )

        return self._client

    def complete(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        json_mode: bool = False,
    ) -> LLMResponse:
        """
        Generate a completion.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            temperature: Override temperature
            max_tokens: Override max tokens
            json_mode: Request JSON output

        Returns:
            LLMResponse with content and metadata
        """
        temperature = temperature or self.llm_config.temperature
        max_tokens = max_tokens or self.llm_config.max_tokens

        self.logger.debug(
            "Generating completion",
            prompt_length=len(prompt),
            json_mode=json_mode,
        )

        provider = self.llm_config.provider

        try:
            if provider == LLMProvider.ANTHROPIC:
                return self._complete_anthropic(
                    prompt, system_prompt, temperature, max_tokens
                )
            elif provider in [LLMProvider.OPENAI, LLMProvider.OLLAMA]:
                return self._complete_openai(
                    prompt, system_prompt, temperature, max_tokens, json_mode
                )
            else:
                return self._complete_litellm(
                    prompt, system_prompt, temperature, max_tokens, json_mode
                )
        except Exception as e:
            raise LLMError(
                f"Completion failed: {str(e)}",
                provider=provider.value,
                model=self.llm_config.model,
            )

    def _complete_anthropic(
        self,
        prompt: str,
        system_prompt: str | None,
        temperature: float,
        max_tokens: int,
    ) -> LLMResponse:
        """Complete using Anthropic's API."""
        client = self._get_client()

        kwargs: dict[str, Any] = {
            "model": self.llm_config.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }

        if system_prompt:
            kwargs["system"] = system_prompt

        response = client.messages.create(**kwargs)

        return LLMResponse(
            content=response.content[0].text,
            model=response.model,
            tokens_used=response.usage.input_tokens + response.usage.output_tokens,
            finish_reason=response.stop_reason,
            metadata={
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            },
        )

    def _complete_openai(
        self,
        prompt: str,
        system_prompt: str | None,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> LLMResponse:
        """Complete using OpenAI's API."""
        client = self._get_client()

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self.llm_config.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = client.chat.completions.create(**kwargs)

        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            tokens_used=response.usage.total_tokens if response.usage else 0,
            finish_reason=choice.finish_reason or "unknown",
            metadata={
                "input_tokens": response.usage.prompt_tokens if response.usage else 0,
                "output_tokens": response.usage.completion_tokens if response.usage else 0,
            },
        )

    def _complete_litellm(
        self,
        prompt: str,
        system_prompt: str | None,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> LLMResponse:
        """Complete using LiteLLM."""
        import litellm

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self.llm_config.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if self.llm_config.api_key:
            kwargs["api_key"] = self.llm_config.api_key

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = litellm.completion(**kwargs)

        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            tokens_used=response.usage.total_tokens if response.usage else 0,
            finish_reason=choice.finish_reason or "unknown",
        )

    def complete_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        """
        Generate a JSON completion.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            schema: Optional JSON schema for validation

        Returns:
            Tuple of (parsed JSON dict, tokens used)
        """
        # Add JSON instruction to system prompt
        json_instruction = "Respond with valid JSON only. No markdown, no explanations."
        if schema:
            json_instruction += f"\n\nExpected schema: {json.dumps(schema)}"

        full_system = f"{system_prompt}\n\n{json_instruction}" if system_prompt else json_instruction

        response = self.complete(
            prompt=prompt,
            system_prompt=full_system,
            json_mode=True,
        )

        try:
            # Parse JSON from response
            content = response.content.strip()
            # Handle potential markdown code blocks
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            parsed = json.loads(content)
            return parsed, response.tokens_used
        except json.JSONDecodeError as e:
            raise LLMError(
                f"Failed to parse JSON response: {str(e)}",
                provider=self.llm_config.provider.value,
                details={"content": response.content[:500]},
            )

    def batch_complete(
        self,
        prompts: list[str],
        system_prompt: str | None = None,
        temperature: float | None = None,
    ) -> list[LLMResponse]:
        """
        Complete multiple prompts.

        Currently processes sequentially. Future: support batch APIs.
        """
        responses = []
        for prompt in prompts:
            response = self.complete(
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=temperature,
            )
            responses.append(response)
        return responses
