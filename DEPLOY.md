# Quill deployment guide

This document describes how to deploy **quill** on a Linux system using **systemd** with secure handling of configuration and secrets.

---

# 1. Create dedicated system user

Create a minimal system account for running the service.

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin quill
```

Creates a system user named `quill` with no login shell and no home directory.
The service runs under this user for security.

---

# 2. Install the binary

Copy the compiled binary to a standard system location

```bash

sudo cp /path/to/quill /usr/local/bin/quill
sudo chmod 755 /usr/local/bin/quill
```

Copies the binary to `/usr/local/bin`. `755` allows execution for all users, owned by `root`.

---

# 3. Create configuration directory

Create the configuration directory.

```bash
sudo mkdir -p /etc/quill
sudo chown root:quill /etc/quill/
sudo chmod 750 /etc/quill/
```

Creates the directory that stores configuration and secrets.

---

# 4. Create configuration file

Create the base configuration file. Remember that "<id>" value we will use later. This should be a string value.

```bash
sudo nano /etc/quill/config-<id>.yaml
```

---

# 5. Set config file permissions

Restrict access to the configuration file.

```bash
sudo chown root:quill /etc/quill/config-<id>.yaml
sudo chmod 640 /etc/quill/config-<id>.yaml
```

`root` owns the file, members of the `quill` group can read it.  Other users cannot access it

---

# 6. Create secrets environment file

Create a file for sensitive environment variables

```bash
sudo nano /etc/quill/quill.env
```

Example contents:

```ini
# Overrides for environment-specific hosts
# DB
QUILL_DB_USER=db-user
QUILL_DB_PASSWORD=db-password
QUILL_DB_HOST=db-instance
QUILL_DB_NAME=db-name
# Kafka
KAFKA_BROKERS=broker-01:9092,broker-02:9092
KAFKA_TOPIC=topic
```

---

# 7. Set secrets file permissions

Restrict access to the secrets file.

```bash
sudo chown root:quill /etc/quill/quill.env
sudo chmod 640 /etc/quill/quill.env
```

Only `root` can read or modify the file

---

# 8. Create systemd service unit

Create the service definition.

```bash
sudo nano /etc/systemd/system/quill@.service
```

Service unit:

```ini
[Unit]
Description=Quill consumer
After=network.target

[Service]
User=quill
Group=quill
EnvironmentFile=/etc/quill/quill.env
ExecStart=/usr/local/bin/quill --config /etc/quill/config-%i.yaml
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=quill-%i

[Install]
WantedBy=multi-user.target
```

The service:

- runs the binary
- loads vars from `/etc/quill/quill.env`
- restarts automatically on failure
- logs to `journald`

---

# 9. Reload systemd

Reload systemd so it detects the new service

```bash
sudo systemctl daemon-reload
```

Systemd scans and registers the new unit file

---

# 10. Enable the service

Enable the service at system startup.

```bash
sudo systemctl enable quill
```

Creates a symlink so the service starts automatically on boot

---

# 11. Start the service

Start the service immediately.

```bash
sudo systemctl start quill@<id>
```

Launches the quill

---

# 12. Check service status

Verify that the service is running

```bash
sudo systemctl status quill@<id>
```

Shows the current state, logs, and exit codes

---

# 13. View logs

View logs from the systemd journal

```bash
journalctl -u quill@<id> -f
```

Displays live logs for the service

---
