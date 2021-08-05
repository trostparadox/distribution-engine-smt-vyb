#/bin/bash

cd /home/ubuntu/scot/distribution-engine
cp server/app.py deployed_app.py
gunicorn --worker-tmp-dir /mem --workers=2 deployed_app:app -b 0.0.0.0:5001
