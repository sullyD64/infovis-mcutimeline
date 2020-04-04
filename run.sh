#!/bin/bash
npm run dev & 
source ./venv/bin/activate; python3 manage.py livereload --ignore-static-dirs &>/dev/null & python3 manage.py runserver