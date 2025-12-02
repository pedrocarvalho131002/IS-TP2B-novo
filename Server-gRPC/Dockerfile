FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install grpcio grpcio-tools lxml

CMD ["python", "server.py"]
