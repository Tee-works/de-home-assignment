#!/usr/bin/env python3
from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run(
        "de_home_assignment.api:create_app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        factory=True,
    )


if __name__ == "__main__":
    main()
