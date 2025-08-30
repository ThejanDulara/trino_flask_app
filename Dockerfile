FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
# Railway will provide $PORT; bind to 0.0.0.0
CMD ["python", "app.py"]
