ARG DIST=/opt

# -------------------
# Build Go executable

FROM gcr.io/cloud-builders/go AS build

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

# -----------------------------------------
# Copy executable into a clean final image

FROM alpine

ARG DIST

COPY --from=build ${DIST} /

ENTRYPOINT ["/main"]
