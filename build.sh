readonly TAG=$1
readonly DOCKER_IMAGE=nachocode/data-preparation-node
sbt assembly && docker build -t "$DOCKER_IMAGE:$TAG" .
