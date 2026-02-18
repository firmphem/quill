Patroni PostgreSQL Cluster Ansible Repository
=========

This repository contains roles and playbooks for configuration management of the Patroni PostgreSQL cluster

Roles
------------

### os 

The OS role is responsible for making operating system configuration changes. It prepares the repository, installs utility packages, and configures firewall access.

### etcd

The etcd role is responsible for configuring and managing the etcd cluster, which plays a crucial role in maintaining the consistency and availability of configuration data across distributed systems. In the context of Patroni, the etcd cluster is used for storing and synchronizing critical configuration settings, such as the current leader (primary) node, replication information, and cluster state. This ensures that all nodes in the Patroni cluster have access to the same configuration data, enabling smooth failovers and cluster management. Proper configuration and management of the etcd cluster are essential for the overall stability and performance of the Patroni-based high availability setup.

the following command can be used to check etcd cluster status

```bash
ETCDCTL_API=3 etcdctl --endpoints=http://10.4.62.89:2379 member list --write-out=table
+------------------+---------+-------+------------------------+------------------------+------------+
|        ID        | STATUS  | NAME  |       PEER ADDRS       |      CLIENT ADDRS      | IS LEARNER |
+------------------+---------+-------+------------------------+------------------------+------------+
| 127753c47ea03dfc | started | etcd3 | http://10.4.62.94:2380 | http://10.4.62.94:2379 |      false |
| 14e2dce6714c631c | started | etcd2 | http://10.4.62.90:2380 | http://10.4.62.90:2379 |      false |
| a26c7a41ef8093ca | started | etcd1 | http://10.4.62.89:2380 | http://10.4.62.89:2379 |      false |
+------------------+---------+-------+------------------------+------------------------+------------+

```


### haproxy

The haproxy role is responsible for configuring and managing the HAProxy and Keepalived services. HAProxy is used to provide high availability and load balancing by distributing incoming traffic across multiple PostgreSQL nodes, ensuring that the system remains accessible even in the event of failures. Keepalived is utilized to manage virtual IPs, enabling seamless failover between PostgreSQL nodes. This combination ensures that the Patroni cluster remains highly available, with automatic failover and load balancing in place to maintain service continuity and minimize downtime. The role ensures both services are correctly configured and running, providing a reliable and fault-tolerant setup.

### patroni-cluster

The patroni-cluster role is responsible for configuring and managing both the Patroni and PostgreSQL clusters. This role ensures that the Patroni service is properly configured to manage the PostgreSQL instances and handle high availability within the cluster. Based on the provided Ansible variables, the role dynamically applies the necessary configuration changes to the Patroni-managed PostgreSQL cluster.

### pmm-server

The pmm-server role is responsible for configuring and managing the PMM (Percona Monitoring and Management) server. This role ensures that the PMM server is correctly installed, configured, and running, enabling centralized monitoring and management of databases in the environment. It integrates with the PMM client to collect performance metrics, query insights, and other database-related data, providing visibility into the health and performance of the database systems. The role configures various PMM components, including the web interface, data storage, and monitoring agents, to ensure efficient monitoring and alerting, thereby helping administrators optimize database performance and troubleshoot issues effectively.


Playbooks
--------------

### pb-configure-database

the pb-configure-database playbook is responsible applying all roles except the pmm-server role. So when we have a configuration change on one of the `os`, `etcd`, `haproxy`, `patroni-cluster` we can just call this playbook to reflect the changes to the inventory

the following command can be used to run this playbook in the check mode. Check mode only checks and shows the changes without changing anything

```bash
ansible-playbook -i env-prod/hosts playbooks/pb-configure-database.yml --check --diff
```

and the following command can be used to limit the playbook with specific inventory servers

```bash
ansible-playbook -i env-prod/hosts playbooks/pb-configure-database.yml --check --diff -l 10.4.62.94 
```

the above commands apply the roles one by one since we have the following structure in the playbook

```yaml
- name: Install PostgreSQL Components
  hosts: postgresql
  become: yes
  roles:
    - role: os
      tags: os
    - role: etcd
      tags: etcd
    - role: haproxy
      tags: haproxy
    - role: patroni-cluster
      tags: postgres

```

If we want to apply only specific roles, we can use tags with the following command, specifying the corresponding tag of the role we want to apply:


```bash
ansible-playbook -i env-prod/hosts playbooks/pb-configure-database.yml --tags=postgres --check --diff -l 10.4.62.94
```

This command allows us to execute only the tasks associated with the postgres tag, performing a dry run (--check) to see what changes would be made, and showing the differences (--diff) without actually applying them. The -l flag restricts the execution to a specific host, in this case, 10.4.62.94. This approach provides a more targeted application of the playbook, allowing for precise control over which parts of the configuration are applied.


### pb-configure-pmm

The pb-configure-pmm playbook is responsible applying pmm-server role.

```bash
ansible-playbook -i env-prod/hosts playbooks/pb-configure-pmm.yml --check --diff 
```



Variables
------------

### Group Variables

Variables are stored in `env-prod/group_vars/all/vars.yml`. All configurable variables are located here, and any changes to their values should be made in this file.

Each role has a specific prefix in the variable names, and the variables are grouped for easier management.

```yaml
...
...
keepalived_floating_ip: 10.4.62.99 
keepalived_interface: ens192


# ETCD Variables
etcd_client_port: 2379
etcd_peer_port: 2380

# Patroni & PostgreSQL Parameters
scope: prod-main
patroni_rest_port: 8008
patroni_peer_port: 8009
patroni_ttl: 100
patroni_loop_wait: 10
patroni_retry_timeout: 10
patroni_maximum_lag_on_failover: 1048576 #1MB
postgresql_synchronous_commit: 'on'
postgresql_synchronous_standby_names: ''
postgresql_port: 5432
postgresql_wal_archive_folder: log_archive 
...
...
```

### Host Variables

Since there is no need for many host variables the inventory file is used for the ones we have insted of creating seperate variable files to keep simplicity.

```yaml
[postgresql]
10.4.62.89 server_ip=10.4.62.89 server_hostname=PROD-PSQL-1 node_name=psql1 etcd_name=etcd1 keepalied_state=MASTER keepalied_weight=100 nofailover=false noloadbalance=false clonefrom=false nosync=false
10.4.62.90 server_ip=10.4.62.90 server_hostname=PROD-PSQL-2 node_name=psql2 etcd_name=etcd2 keepalied_state=BACKUP keepalied_weight=96 nofailover=false noloadbalance=false clonefrom=false nosync=false
10.4.62.94 server_ip=10.4.62.94 server_hostname=PROD-PSQL-3 node_name=psql3 etcd_name=etcd3 keepalied_state=BACKUP keepalied_weight=92 nofailover=false noloadbalance=false clonefrom=false nosync=false
```

Secrets
-------

Secrets are stored in `env-prod/group_vars/all/vault.yml` in an encrypted format. Encryption and decryption are handled using `ansible-vault`, and the vault key should be provided in the `vault.password` file located in the root directory. This file is included in the `.gitignore` to prevent it from being pushed to the Git repository. The password will be shared separately.


```bash
head env-prod/group_vars/all/vault.yml                                                                      
$ANSIBLE_VAULT;1.1;AES256
32623635383539366662666537336563656464306464613166346365383137323138373465343037
6233393031393666366334643661663061636137393138330a656664303063333432636639353336
63643563316438336537306461613963656263336362313664623433656130393132356234646331
3262376234333833380a636632333538316661343035363138613234323231656362623865396162
38386134383862623038633930343334396462353738303833383061646636356434626231306537
32313433313163623966626433626562343131643131316439663035653338393534616338333533
65626361353865633637616437383664303235353362366331306535323830373861663430326232
37366139613864383362353832656436353636383537653264326264653662663338363965616364
32633039396435656134643866376237356265373332376239646538376562633832393734663934
````

To decrypt the encrypted file we can use the following command,

```bash
ansible-vault decrypt env-prod/group_vars/all/vault.yml
```

To encrypt back,

```bash
ansible-vault encrypt env-prod/group_vars/all/vault.yml
```

Backups
-------
Backup script template is stored in `roles/patroni-cluster/templates/full_backup.sh.j2` and this is installed to each database node. However the backup cronjob is configured based on the `backup_cron_enabled` host variable. If this is enabled on a host it means the cron will run daily and get backup from that instance.

Backup related parameters can be found in the `env-prod/group_vars/all/vars.yml`

```bash
postgresql_backup_folder: /var/lib/pgsql/17/backups # Change to backup volume once it is available
postgresql_backup_retention: 7
postgresql_basebackup_max_rate: 200M

```


Patroni Commands
-------

When we have a configuration change we should first apply the change with the ansible playbook but this change will not take affect immediately. We need to reload thhese changes with patroni interface.

The following commands can be applied one of the database instances.

Patroni provices a utility tool which is called patronictl that allows us to perform operations.

### List
This command allows us to list the current cluster status. We can see which instance is master and which are the replicas and the replication lag if we have.

```bash
patronictl -c /etc/patroni/patroni.yml list prod-main
+ Cluster: prod-main (7447229137066097539) -+----+-----------+
| Member | Host       | Role    | State     | TL | Lag in MB |
+--------+------------+---------+-----------+----+-----------+
| psql1  | 10.4.62.89 | Leader  | running   | 25 |           |
| psql2  | 10.4.62.90 | Replica | streaming | 25 |         0 |
| psql3  | 10.4.62.94 | Replica | streaming | 25 |         0 |
+--------+------------+---------+-----------+----+-----------+
```

### Reload

The reload command applies config changes to the database instance. It can be applied to a specific node or all of them at once,

```bash
patronictl -c /etc/patroni/patroni.yml reload prod-main psql3
+ Cluster: prod-main (7447229137066097539) -+----+-----------+
| Member | Host       | Role    | State     | TL | Lag in MB |
+--------+------------+---------+-----------+----+-----------+
| psql1  | 10.4.62.89 | Leader  | running   | 25 |           |
| psql2  | 10.4.62.90 | Replica | streaming | 25 |         0 |
| psql3  | 10.4.62.94 | Replica | streaming | 25 |         0 |
+--------+------------+---------+-----------+----+-----------+
Are you sure you want to reload members psql3? [y/N]:

```

to apply all of them at once,

```bash
patronictl -c /etc/patroni/patroni.yml reload prod-main
+ Cluster: prod-main (7447229137066097539) -+----+-----------+
| Member | Host       | Role    | State     | TL | Lag in MB |
+--------+------------+---------+-----------+----+-----------+
| psql1  | 10.4.62.89 | Leader  | running   | 25 |           |
| psql2  | 10.4.62.90 | Replica | streaming | 25 |         0 |
| psql3  | 10.4.62.94 | Replica | streaming | 25 |         0 |
+--------+------------+---------+-----------+----+-----------+
Are you sure you want to reload members psql1, psql2, psql3? [y/N]:
```

### Restart

Some configuration changes requires database restarts and when we list the cluster members Patroni shows an information that indicates if a server has a config change that requires server restart. So when we need to restart a PostgreSQL database instance we can use this command. Restarting the leader node does not trigger failover automatically. So we should first restart the replicas and then perform a switchover and then restart the old primary.

```bash
patronictl -c /etc/patroni/patroni.yml restart prod-main psql3
+ Cluster: prod-main (7447229137066097539) -+----+-----------+
| Member | Host       | Role    | State     | TL | Lag in MB |
+--------+------------+---------+-----------+----+-----------+
| psql1  | 10.4.62.89 | Leader  | running   | 25 |           |
| psql2  | 10.4.62.90 | Replica | streaming | 25 |         0 |
| psql3  | 10.4.62.94 | Replica | streaming | 25 |         0 |
+--------+------------+---------+-----------+----+-----------+
When should the restart take place (e.g. 2025-01-01T20:57)  [now]:
```

### Switchover

This command is used to change the current leader gracefully. If the current leader is lost all of a sudden then the Patroni steps in and performs the failover, if we want to change the leader then we can call the switchover command.

```bash
patronictl -c /etc/patroni/patroni.yml switchover prod-main
Current cluster topology
+ Cluster: prod-main (7447229137066097539) -+----+-----------+
| Member | Host       | Role    | State     | TL | Lag in MB |
+--------+------------+---------+-----------+----+-----------+
| psql1  | 10.4.62.89 | Leader  | running   | 25 |           |
| psql2  | 10.4.62.90 | Replica | streaming | 25 |         0 |
| psql3  | 10.4.62.94 | Replica | streaming | 25 |         0 |
+--------+------------+---------+-----------+----+-----------+
Primary [psql1]:
```

### Pause

This command is used to pause patroni if we need to do some manual operations and if we want to the Patroni not touch the PostgreSQL cluster.

```bash
patronictl -c /etc/patroni/patroni.yml pause prod-main
Success: cluster management is paused
```

### Resume

This command resumes a paused patroni cluster.

```bash
patronictl -c /etc/patroni/patroni.yml resume prod-main
Success: cluster management is resumed
```