
# First rebuild the packages.
FROM node:9-alpine
COPY . /opt/swl

# Rebuild everything
RUN apk update && apk add g++ make sqlite-dev python && npm i -g yarn && cd /opt/swl && rm -rf node_modules && yarn install --production


# Build a trimmed down image with just the code and compiled items.
FROM node:9-alpine
COPY --from=0 /opt/swl /opt/swl
ENTRYPOINT ["/opt/swl/node_modules/.bin/swl"]
