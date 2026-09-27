#ifndef PGOS_PROBE_HTTP_H
#define PGOS_PROBE_HTTP_H

#include "postgres.h"

#define PGOS_MAX_REQUEST (1024 * 1024)
#define PGOS_MAX_RESPONSE (8 * 1024 * 1024)

/* HTTPS only, no redirects/application retries. Returned JSON text is palloc'd. */
extern char *pgos_http_post(const char *url, const char *api_key,
                           const char *body, int timeout_ms, const char *ca_file);
/* A fresh, isolated connection with no API headers or credentials. */
extern char *pgos_http_get(const char *url, int timeout_ms, const char *ca_file);

#endif
