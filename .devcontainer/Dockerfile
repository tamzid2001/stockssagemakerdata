FROM mcr.microsoft.com/vscode/devcontainers/python:0-3.11

# Install git-lfs (optional for large models/data) and clean up
RUN apt-get update \
 && apt-get install -y --no-install-recommends git-lfs \
 && rm -rf /var/lib/apt/lists/*

USER vscode
