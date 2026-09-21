mods = ["pytest", "pydantic", "pydantic_settings", "structlog", "sqlalchemy", "httpx"]
for m in mods:
    try:
        __import__(m)
        print("OK  ", m)
    except Exception as e:
        print("MISS", m, "->", type(e).__name__)
