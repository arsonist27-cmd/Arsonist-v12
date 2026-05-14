Place TLS certificate files here for production:

- `fullchain.pem`
- `privkey.pem`

For local testing you can create self-signed certs:

```bash
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout deploy/nginx/certs/privkey.pem \
  -out deploy/nginx/certs/fullchain.pem \
  -subj "/CN=localhost"
```
