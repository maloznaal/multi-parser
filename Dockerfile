FROM golang:1.13-alpine
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
RUN mkdir tmp
COPY . .
RUN go build -o main .
CMD ["./main"]