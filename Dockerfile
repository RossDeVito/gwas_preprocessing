# FROM python:3.10-slim-bullseye
FROM python:3.10-bullseye

# Install dependencies
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && apt-get install -y openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/* && \
    pip --no-cache-dir install hail ipython

ENTRYPOINT ["/bin/bash"]
CMD []