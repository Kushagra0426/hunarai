# One image, run as every node. Identity comes from NODE_ID at runtime, so
# nothing here is node-specific.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Requirements first: this layer only rebuilds when dependencies change, not on
# every source edit.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Non-root. Nothing here needs privileges, and a container that cannot write to
# its own code is one less thing to think about.
RUN useradd --create-home --uid 1000 app && chown -R app:app /app
USER app

EXPOSE 8000

# --lifespan on is required, not cosmetic: the heartbeat and reaper start from
# the ASGI lifespan, and without them dead nodes are never reaped.
CMD ["uvicorn", "connmgr.asgi:application", \
     "--host", "0.0.0.0", "--port", "8000", "--lifespan", "on"]
