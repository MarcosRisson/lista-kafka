FROM python:3.8-slim

# Instalar as bibliotecas necessárias
RUN pip install confluent_kafka flask

# Copiar o código-fonte para o contêiner
WORKDIR /producer
COPY . .

# Comando padrão para executar o produtor
CMD ["python", "producer.py"]
