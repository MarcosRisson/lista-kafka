# Dockerfile para o consumidor Node.js
FROM node:14

WORKDIR /consumer
COPY . .

RUN npm install
RUN npm install nodemon -g

CMD ["nodemon", "consumer.js"]
