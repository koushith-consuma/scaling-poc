# Next.js chat frontend — separate service.
FROM node:20-slim
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .
RUN npm run build

ENV NODE_ENV=production
EXPOSE 3001
# BACKEND_URL is read at runtime by next.config rewrites.
CMD ["npm", "run", "start"]
