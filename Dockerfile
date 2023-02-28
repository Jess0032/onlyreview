# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.11

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY . /app

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

VOLUME ["/app/data"]

# Start the application
CMD ["python", "main.py"]
