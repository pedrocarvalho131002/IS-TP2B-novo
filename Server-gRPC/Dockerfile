FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y default-jre wget

RUN wget https://repo1.maven.org/maven2/net/sf/saxon/Saxon-HE/10.9/Saxon-HE-10.9.jar -O /app/saxon-he.jar

COPY . /app/

RUN pip install lxml grpcio grpcio-tools

EXPOSE 50051

CMD ["python", "server.py"]
