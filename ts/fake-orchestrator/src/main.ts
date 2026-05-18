#!/usr/bin/env node --enable-source-maps

import { Hono } from "hono";
import { serve } from "@hono/node-server";

/**
 * Jobs are "started" with PUT and are added to the map with value 1 if they
 * don't exist yet. if they do already exist then the status is not changed.
 * They are marked as "done" with a POST and their status is set to 2, throwing
 * if they weren't started yet.
 * Jobs are queried at any time with GET, 0 for unstarted, 1 for started, 2 for
 * finished.
 */
const main = () => {
  const jobs: Map<string, number> = new Map<string, number>();
  const app = new Hono();
  app.get("/jobs", (c) => {
    return c.json({ jobs: Object.fromEntries(jobs.entries()) });
  });
  app.put("/:id", (c) => {
    const id = c.req.param("id");
    if (jobs.has(id)) {
      return c.json({ status: jobs.get(id) });
    }
    jobs.set(id, 1);
    return c.json({ status: 1, id });
  });
  app.post("/:id", (c) => {
    const id = c.req.param("id");
    // not yet started, or should never be zero
    if (!jobs.has(id) || jobs.get(id) === 0) {
      return c.json({ status: -1 });
    }
    jobs.set(id, 2);
    return c.json({ status: 2, id });
  });
  app.get("/:id", (c) => {
    const id = c.req.param("id");
    const job = jobs.get(id);
    return c.json({ status: job ?? 0, id });
  });
  serve({
    fetch: app.fetch,
    port: 9991, // TODO: take port arg
  });
};

main();
