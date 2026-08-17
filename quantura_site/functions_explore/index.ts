import express from "express";
import { quanturaExploreApi, shopApi } from "./src/index";

const app = express();

// The shop router must run first because the main API has a terminal 404 handler.
app.use(shopApi);
app.use(quanturaExploreApi);

export default app;
