FROM node:6.2

ENV REDIS_HOST localhost
ENV REDIS_PORT 6379

COPY package.json .
COPY index.js .

RUN npm install

EXPOSE 7000