#!/bin/bash
rm -rf mcu_app/migrations
./manage.py reset_db --noinput
./manage.py makemigrations
./manage.py migrate
echo "from django.contrib.auth.models import User; User.objects.create_superuser('root', '', 'root')" | ./manage.py shell
./manage.py makemigrations mcu_app
./manage.py migrate mcu_app
cp initdb/0002_importdata.py mcu_app/migrations/0002_importdata.py
./manage.py migrate mcu_app 0002_importdata