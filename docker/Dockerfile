FROM node:6.2

ENV REDIS_HOST localhost
ENV REDIS_PORT 6379

RUN mkdir /app
WORKDIR /app

COPY package.json /app/
COPY index.js /app/

RUN cd /app
RUN npm install

EXPOSE 7000

CMD ["node", "index.js"]