FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml requirements.txt main.py /app/
COPY etl/ /app/etl/

RUN pip install --no-cache-dir -r requirements.txt .

COPY config/ /app/config/

# Define the entry point for the container
ENTRYPOINT ["python", "main.py"]

# Default command arguments
CMD ["config.yml"]
