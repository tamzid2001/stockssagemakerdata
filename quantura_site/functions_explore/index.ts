import "@aikidosec/firewall";
import express from "express";
import helmet from "helmet";
import { quanturaExploreApi, shopApi } from "./src/index";

const app = express();
app.disable("x-powered-by");
app.use(helmet());

// The shop router must run first because the main API has a terminal 404 handler.
app.use(shopApi);
app.use(quanturaExploreApi);

export default app;
