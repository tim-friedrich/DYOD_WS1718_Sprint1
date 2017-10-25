FROM ubuntu:zesty

RUN apt-get update
RUN apt-get install -y git sudo vim

# Build this image: docker build . -t dyod:sprint1

# You will be likely to clone the repository on the host.
# Do not forget to run install.sh inside the container.

# Initially run this image: docker run -ti --name sprint1 -v $(pwd):/develop
# Run the created container: docker start -i sprint1

