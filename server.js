require("dotenv").config();
const Fastify = require("fastify");

const app = Fastify({ logger: true });

app.get("/health", async () => ({ ok: true }));
app.get("/", async () => ({ message: "Catalog backend running" }));

const port = Number(process.env.PORT || 3000);
app.listen({ port, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});