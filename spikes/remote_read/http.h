#ifndef PGOS_PROBE_HTTP_H
#define PGOS_PROBE_HTTP_H

#include "postgres.h"

#define PGOS_MAX_REQUEST (1024 * 1024)
#define PGOS_MAX_RESPONSE (8 * 1024 * 1024)

/* HTTPS POST only, no redirects/retries. Caller owns the returned palloc buffer. */
extern char *pgos_http_post(const char *url, const char *api_key,
                           const char *body, int timeout_ms, const char *ca_file);

#endif
