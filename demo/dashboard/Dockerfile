FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY scenarios/ scenarios/
COPY static/ static/
CMD ["python", "app.py"]
