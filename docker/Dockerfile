FROM node:8

ENV REDIS_HOST localhost
ENV REDIS_PORT 6379
ENV NODE_ENV production
ENV UV_THREADPOOL_SIZE 10

RUN mkdir /app
WORKDIR /app

COPY package.json /app/
COPY index.js /app/

RUN cd /app
RUN npm install

EXPOSE 8080

CMD ["node", "index.js"]