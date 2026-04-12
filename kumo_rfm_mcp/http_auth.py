import os
from typing import Final

from fastmcp.server.auth.auth import AccessToken, TokenVerifier

MCP_BEARER_TOKEN_ENV: Final[str] = 'MCP_BEARER_TOKEN'


class StaticBearerTokenAuth(TokenVerifier):
    """Authenticate MCP HTTP requests with a shared bearer token."""
    def __init__(self, token: str) -> None:
        super().__init__()
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        if token != self._token:
            return None

        return AccessToken(
            token=token,
            client_id='snowflake-native-app',
            scopes=[],
        )


def get_http_auth() -> StaticBearerTokenAuth | None:
    """Build HTTP auth from environment variables if configured."""
    token = os.getenv(MCP_BEARER_TOKEN_ENV)
    if token is None:
        return None

    token = token.strip()
    if not token:
        return None

    return StaticBearerTokenAuth(token=token)
