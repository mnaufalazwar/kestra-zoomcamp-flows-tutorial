# kestra-zoomcamp-flows-tutorial

to run kestra on google cloud compute engine:
1. Create VM instance: min 2 cpu and 4 GB memory.
2. Set firewall rule to allow connection to kestra then add it to network tags.
3. ssh to instance, install docker and docker compose.
4. create postgres at google cloud -> allow private IP
5. create bucket -> crete service account to access bucket
6. use this docker-compose.yml : 
```
volumes:
  postgres-data:
    driver: local
  kestra-data:
    driver: local

services:
  kestra:
    image: kestra/kestra:latest
    pull_policy: always
    # Note that this setup with a root user is intended for development purpose.
    # Our base image runs without root, but the Docker Compose implementation needs root to access the Docker socket
    user: "root"
    command: server standalone
    volumes:
      - kestra-data:/app/storage
      - /var/run/docker.sock:/var/run/docker.sock
      - /tmp/kestra-wd:/tmp/kestra-wd
    environment:
      SECRET_GITHUB_ACCESS_TOKEN: <ENCODE BASE64 GITHUB ACCESS TOKEN>
      SECRET_GCP_CREDS: <ENCODE BASE64 SERVICE ACCOUNT TO ACCESS BUCKET AND BIG QUERY FOR DATA PIPELINE>
      KESTRA_CONFIGURATION: |
        datasources:
          postgres:
            url: jdbc:postgresql://10.114.112.3:5432/postgres
            driverClassName: org.postgresql.Driver
            username: kestra
            password: Admin@k3str4
        kestra:
          server:
            basicAuth:
              enabled: true
              username: admin@kestra.io # it must be a valid email address
              password: Admin1234 # it must be at least 8 characters long with uppercase letter and a number
          repository:
            type: postgres
          storage:
            type: gcs
            gcs:
              bucket: kestra-storage-falazwar
              project-id: zoomcamp-468211
              serviceAccount: <SERVICE ACCOUNT TO BUCKET FOR KESTRA STOGARE IN ONE LINE (STRINGIFY) INSIDE "">
          queue:
            type: postgres
          tasks:
            tmpDir:
              path: /tmp/kestra-wd/tmp
          url: http://localhost:8080/
    ports:
      - "8080:8080"
      - "8081:8081"
```

7. encode creds:
- if it is file: 
-- on linux :
printf '%s' "$ENCODED" | base64 -D | jq . >/dev/null && echo OK || echo BAD
Encode, single line :base64 -w 0 gcp-creds-taxi.json
-- on mac: base64 < your-sa.json | tr -d '\n'
              
- if it is string: echo -n "BLABLABLA" | base64

8. stringify creds:
   cat blablabla.json | jq '@json'
