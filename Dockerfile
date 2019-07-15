ARG DIST=/opt

# -------------------
# Build Go executable

FROM golang AS build

ARG DIST

WORKDIR /build

# download dependencies
ADD go.* ./
RUN go mod download
RUN go list -export $(go list -m)/...

# compile main executable
ADD *.go ./
RUN go build -o main
RUN cp main ${DIST}

# ---------------------------------------------
# Copy all executables into a clean final image

FROM debian:testing-slim

ARG DIST

WORKDIR ${DIST}

COPY --from=build ${DIST} .

ENTRYPOINT ["./main"]
