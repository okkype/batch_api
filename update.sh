#!/bin/bash
git pull origin master
python manage.py makemigrations
python manage.py migrate
