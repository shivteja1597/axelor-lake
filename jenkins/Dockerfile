FROM jenkins/jenkins:lts-jdk21

USER root

# Install Python3, venv, and Docker CLI so they persist across container restarts
RUN apt-get update && \
    apt-get install -y python3 python3-venv docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER jenkins
