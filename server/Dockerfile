FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /weir-server .

FROM alpine:3.20
COPY --from=build /weir-server /usr/local/bin/weir-server
EXPOSE 4100
ENTRYPOINT ["weir-server"]
