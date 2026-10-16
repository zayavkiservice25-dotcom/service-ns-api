# STAGING FIRST. Uses a single Render web service, Node.js + PHP CLI 8.2.
FROM node:22-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
  php8.2-cli php8.2-common libltdl7 libpcsclite1 zlib1g ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json package-lock.json* ./
# Use a lockfile when available, otherwise install dependencies from package.json.
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev --no-audit --no-fund; fi
COPY . .
# Put the licensed binary into private build context only; never commit it to public Git.
COPY kalkan/kalkancrypt.so /opt/kalkan/kalkancrypt.so
RUN chmod 0644 /opt/kalkan/kalkancrypt.so \
 && php -d extension=/opt/kalkan/kalkancrypt.so -r 'if (!function_exists("KalkanCrypt_Init") || !function_exists("KalkanCrypt_VerifyData")) {fwrite(STDERR,"KalkanCrypt failed to load\n");exit(1);} echo "KalkanCrypt available\n";'
ENV NODE_ENV=production
CMD ["node", "server.js"]
