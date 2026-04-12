import os

import pytest
from fastmcp.exceptions import ToolError
from kumoai.experimental import rfm

from kumo_rfm_mcp import SessionManager
from kumo_rfm_mcp.tools.auth import authenticate


@pytest.mark.asyncio
async def test_authenticate_with_api_key(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('KUMO_API_KEY', raising=False)
    called = {'value': False}

    def fake_init() -> None:
        called['value'] = True

    monkeypatch.setattr(rfm, 'init', fake_init)

    result = await authenticate(api_key='secret-key')

    assert result == "KumoRFM session successfully authenticated"
    assert os.environ['KUMO_API_KEY'] == 'secret-key'
    assert called['value'] is True


@pytest.mark.asyncio
async def test_authenticate_rejects_empty_api_key(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('KUMO_API_KEY', raising=False)

    with pytest.raises(ToolError, match='Provided API key is empty'):
        await authenticate(api_key='   ')


@pytest.mark.asyncio
async def test_authenticate_uses_browser_flow_without_api_key(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('KUMO_API_KEY', raising=False)
    SessionManager.get_default_session().clear()
    called: dict[str, str | bool | None] = {'api_url': None, 'init': False}

    def fake_authenticate(api_url: str | None = None) -> None:
        called['api_url'] = api_url
        os.environ['KUMO_API_KEY'] = 'browser-key'

    def fake_init() -> None:
        called['init'] = True

    monkeypatch.setattr(rfm, 'authenticate', fake_authenticate)
    monkeypatch.setattr(rfm, 'init', fake_init)

    result = await authenticate(api_url='https://kumorfm.ai')

    assert result == "KumoRFM session successfully authenticated"
    assert called == {
        'api_url': 'https://kumorfm.ai',
        'init': True,
    }
