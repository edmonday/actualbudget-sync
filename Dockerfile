FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json pnpm-lock.yaml ./
RUN apk add g++ make python3
RUN corepack pnpm install --frozen-lockfile

FROM node:22-alpine AS builder
WORKDIR /app
COPY . .
COPY --from=deps /app/node_modules ./node_modules
RUN corepack enable
RUN pnpm build 
RUN pnpm prune --prod


FROM node:22-alpine AS runner
WORKDIR /app

ENV NODE_ENV production

COPY package.json ./
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules

ENTRYPOINT ["node", "dist/main.js"]
