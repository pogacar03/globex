"""Create a private, ignored local identity file; never print credentials."""
import json
import os
from pathlib import Path
import secrets
import shlex


def main():
    target = Path(__file__).resolve().parents[1] / "data" / "dev-identity.env"
    target.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(32)
    directory = {token: {"tenant_id": "local-development", "subject": "developer",
        "permissions": ["intent:submit", "commerce:read", "commerce:write"]}}
    content = "export GLOBEX_AUTH_TOKENS=" + shlex.quote(json.dumps(directory)) + "\n"
    content += "export GLOBEX_ACCESS_TOKEN=" + shlex.quote(token) + "\nexport LLM_MODE=mock\n"
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(content)
    print(f"Created private local identity: {target}. Source it in the API, Worker and client shells.")


if __name__ == "__main__":
    main()
