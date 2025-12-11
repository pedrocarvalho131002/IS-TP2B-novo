FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install grpcio grpcio-tools lxml

EXPOSE 50051

CMD ["python", "server.py"]
