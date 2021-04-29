# distribution-engine
Indexing for Hive Engine Comments Smart Contract

## Installation of packages for Ubuntu 18.04

```
sudo apt-get install postgresql postgresql-contrib python3-pip libicu-dev build-essential libssl-dev python3-dev git

# versioned python packages
sudo apt-get install python3.8-dev
```

## Server

```
sudo apt install nginx ufw php7.2-fpm php7.2-pgsql
```
adminer
```
sudo mkdir /usr/share/adminer
sudo wget "https://www.adminer.org/latest.php" -O /usr/share/adminer/latest.php
sudo ln -s /usr/share/adminer/latest.php /usr/share/adminer/adminer.php
sudo ln -s /usr/share/adminer/adminer.php /var/www/html
```

## Installation of python packages

**General Warning**: Be aware of which pip version is used. If straddling versions, you may have pip3, pip3.6, pip3.7. or use python3.x -m pip.

(Block streaming is using scot user)
```
sudo apt-get install -y python3-setuptools
sudo apt-get install -y python3.8-dev
python3.8 -m pip install wheel beem dataset psycopg2-binary secp256k1prp steemengine base36
```

(API on machine is using root to run gunicorn)
```
sudo su
python3.8 -m pip install gunicorn flask flask-cors flask-compress flask-caching prettytable pytz 
python3.8 -m pip install wheel beem dataset psycopg2-binary secp256k1prp steemengine base36 sqltap simplejson
python3 setup.py install
```

NOTE: In some cases, need --user setting for the module to be visible to the api server (if running official setup),
e.g. `sudo su; python3.8 -m pip install --user flask-compress flask-caching`

Note the config cache directory (config.json), make sure that directory is set up and accessible.

## /mem
```
sudo cp /etc/fstab /etc/fstab.orig
sudo mkdir /mem
echo 'tmpfs       /mem tmpfs defaults,size=64m,mode=1777,noatime,comment=for-gunicorn 0 0' | sudo tee -a /etc/fstab
sudo mount /mem
```

## Create engine user
```
adduser engine
```

## Setup of the postgresql database

Set a password and a user for the postgres database:

```
su postgres
psql -c "\password"
createdb engine
```

## Prepare the postgres database
```
psql -d engine -a -f sql/engine.sql
```

## Config file for accessing the database and the beem wallet
A `config.json` file must be stored in the main directory and in the homepage directory where the `app.py` file is.
```
{
        "databaseConnector": "postgresql://postgres:password@localhost/engine",
        "wallet_password": "abc",
        "flask_secret_key": "abc"
}
```

## Running the scripts
```
chmod a+x run-engine.sh
./run-engine.sh
export FLASK_APP=server/app; export PYTHONPATH=/path/to/distribution-engine/ ; flask run --port 5001
```
or copy the systemd service file to /etc/systemd/system and start it by
```
systemctl start engine
```

and 

```
systemctl start engine-issue
```

and 


```
systemctl start engineserver
```

do the same with run-hive-engine.sh,  hive-engine.

### Backup Node Details

If spinning this up on a different server, here's how to run a backup in parallel.

(Only creates indices necessary for the node. This also wipes the DB)
```
psql postgresql://postgres:pa9lq30m@localhost/engine -a -f sql/engine_noindex.sql 
```

This does a data-only dump
```
pg_dump --data-only -h 95.216.22.185 -p 5432 engine -U postgres --password | psql postgresql://postgres:password@localhost/engine
```

After this, can start the server with `run-engine.sh`

### Continuous Archival

Set up as per https://www.postgresql.org/docs/current/continuous-archiving.html

Specific to main SCOT server:

```
sudo mkdir /media/scotdata/pg_backups/backup_20210308

sudo chown postgres /media/scotdata/pg_backups/backup_20210308

sudo chgrp postgres /media/scotdata/pg_backups/backup_20210308

sudo su postgres

pg_basebackup -D /media/scotdata/pg_backups/backup_20210308 -Ft -z -P

exit  # exit the postgres user

scp -P 24659 -r /media/scotdata/pg_backups/backup_20210308 henodemin@135.181.78.238:pg_backups/.
```

Probably can consider deleting the WAL on occasion, as it takes a lot of space. Or just do
logical backups.

```
