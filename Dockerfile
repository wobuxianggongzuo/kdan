FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc 

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"] 