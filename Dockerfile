# Go back to the image you already have cached
FROM python:3.9-slim

# --- FIX: Switch Debian repositories to a stable snapshot ---
# This fixes the "404 Not Found" / "Trixie" errors without downloading a new image
RUN echo "deb http://deb.debian.org/debian bookworm main" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian-security bookworm-security main" >> /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian bookworm-updates main" >> /etc/apt/sources.list

# Now run the install
RUN apt-get update --allow-releaseinfo-change && \
    apt-get install -y \
        default-jre-headless \
        procps \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*


# 3. Set JAVA_HOME dynamically
# This command asks Linux where it put the default java, so we don't have to guess
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 4. Create a working directory
WORKDIR /app

# 5. Copy requirements and install
COPY requirements.txt .
# We add --default-timeout=100 to give it more time to download
RUN pip install --default-timeout=1000 --no-cache-dir -r requirements.txt 

# 6. Copy the rest of the application
COPY . .

# 7. Default command
CMD ["python", "processing/spark_streaming.py"]